package lint

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// DiffCmd is the Kong CLI struct for the diff command.
// It diffs two schemas, shows the generated DDL, and lints the changes.
type DiffCmd struct {
	// Source of existing schema (exactly one required)
	SourceDSN string `help:"MySQL DSN for existing schema" xor:"source" required:"" env:"MYSQL_DSN"`
	SourceDir string `help:"Directory of CREATE TABLE .sql files for existing schema" xor:"source" required:"" type:"existingdir"`

	// Target / proposed changes (exactly one required)
	TargetDSN   string   `help:"MySQL DSN for target schema (will diff against source)" xor:"target" required:""`
	TargetDir   string   `help:"Directory of CREATE TABLE .sql files for target state" xor:"target" required:"" type:"existingdir"`
	TargetAlter []string `help:"ALTER TABLE statement(s) to apply" short:"a" xor:"target" required:""`

	// Filtering
	IgnoreTables string `help:"Regex pattern of table names to ignore" default:""`
}

// Run executes the diff command. It is called by Kong.
// The output is valid SQL: lint violations appear as SQL comments at the top,
// followed by the DDL statements. This allows the output to be piped directly
// into mysql.
func (cmd *DiffCmd) Run() error {
	ctx := context.Background()

	// 1. Load source schema
	source, err := loadSource(ctx, cmd.SourceDSN, cmd.SourceDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading source schema: %s\n", err)
		os.Exit(2)
	}

	// 2. Load changes
	changes, err := cmd.loadChanges(ctx, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading changes: %s\n", err)
		os.Exit(2)
	}

	// 3. Build config — lint only changed tables
	config, err := buildIgnoreTablesConfig(cmd.IgnoreTables, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error building config: %s\n", err)
		os.Exit(2)
	}
	config.LintOnlyChanges = true

	// 4. Run linters
	violations, err := RunLinters(source, changes, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running linters: %s\n", err)
		os.Exit(2)
	}

	// 5. Print output as valid SQL: violations as comments, then DDL
	printViolationsAsSQL(violations)
	if len(violations) > 0 && len(changes) > 0 {
		fmt.Println()
	}
	printDiff(changes)

	// 6. Exit code
	if HasErrors(violations) {
		os.Exit(1)
	}

	return nil
}

// loadChanges loads the proposed changes from ALTER statements, a target directory, or a target DSN.
func (cmd *DiffCmd) loadChanges(ctx context.Context, source []*statement.CreateTable) ([]*statement.AbstractStatement, error) {
	if len(cmd.TargetAlter) > 0 {
		return loadAlterChanges(cmd.TargetAlter)
	}

	// Load target schema (from dir or DSN) and diff against source
	var target []*statement.CreateTable
	var err error
	if cmd.TargetDir != "" {
		target, err = LoadSchemaFromDir(cmd.TargetDir)
	} else {
		target, err = LoadSchemaFromDSN(ctx, cmd.TargetDSN)
	}
	if err != nil {
		return nil, err
	}

	return diffSchemas(source, target)
}

// loadAlterChanges parses ALTER TABLE statements provided via --target-alter.
func loadAlterChanges(alters []string) ([]*statement.AbstractStatement, error) {
	var changes []*statement.AbstractStatement
	for _, alter := range alters {
		stmts, err := statement.New(alter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ALTER statement %q: %w", alter, err)
		}
		changes = append(changes, stmts...)
	}
	return changes, nil
}

// diffSchemas compares source and target schemas and produces statements
// representing the changes needed to transform source into target.
func diffSchemas(source, target []*statement.CreateTable) ([]*statement.AbstractStatement, error) {
	sourceMap := make(map[string]*statement.CreateTable, len(source))
	for _, ct := range source {
		sourceMap[ct.TableName] = ct
	}

	targetMap := make(map[string]*statement.CreateTable, len(target))
	for _, ct := range target {
		targetMap[ct.TableName] = ct
	}

	var changes []*statement.AbstractStatement

	// Tables in target: diff against source or treat as new
	for name, targetTable := range targetMap {
		sourceTable, exists := sourceMap[name]
		if !exists {
			// New table — restore the AST to SQL and produce a CREATE TABLE as the change
			createSQL, err := restoreCreateTable(targetTable)
			if err != nil {
				return nil, fmt.Errorf("failed to restore CREATE TABLE for new table %s: %w", name, err)
			}
			stmts, err := statement.New(createSQL)
			if err != nil {
				return nil, fmt.Errorf("failed to parse CREATE TABLE for new table %s: %w", name, err)
			}
			changes = append(changes, stmts...)
			continue
		}

		// Existing table — diff it
		diffs, err := sourceTable.Diff(targetTable, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to diff table %s: %w", name, err)
		}
		changes = append(changes, diffs...)
	}

	// Tables in source but not in target — produce DROP TABLE
	for name := range sourceMap {
		if _, exists := targetMap[name]; !exists {
			stmts, err := statement.New(fmt.Sprintf("DROP TABLE `%s`", name))
			if err != nil {
				return nil, fmt.Errorf("failed to parse DROP TABLE for %s: %w", name, err)
			}
			changes = append(changes, stmts...)
		}
	}

	return changes, nil
}

// restoreCreateTable converts a parsed CreateTable back to a SQL string
// using the TiDB AST restore functionality.
func restoreCreateTable(ct *statement.CreateTable) (string, error) {
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return "", fmt.Errorf("failed to restore CREATE TABLE: %w", err)
	}
	return sb.String(), nil
}

// printViolationsAsSQL prints violations as SQL comments, sorted by table then severity.
func printViolationsAsSQL(violations []Violation) {
	if len(violations) == 0 {
		return
	}

	sorted := sortViolations(violations)
	for _, v := range sorted {
		fmt.Printf("-- %s\n", v.String())
	}
}

// printDiff prints the DDL statements as valid SQL.
func printDiff(changes []*statement.AbstractStatement) {
	if len(changes) == 0 {
		fmt.Println("-- No schema differences found.")
		return
	}

	// Sort changes by table name for consistent output
	sorted := make([]*statement.AbstractStatement, len(changes))
	copy(sorted, changes)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Table < sorted[j].Table
	})

	for _, ch := range sorted {
		fmt.Printf("%s;\n", strings.TrimSuffix(ch.Statement, ";"))
	}
}
