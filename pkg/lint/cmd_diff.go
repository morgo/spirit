package lint

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
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

	// 1. Load source schema.
	source, err := loadSource(ctx, cmd.SourceDSN, cmd.SourceDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading source schema: %s\n", err)
		os.Exit(2)
	}

	// 2. Build lint config from flags.
	config, err := buildIgnoreTablesConfig(cmd.IgnoreTables, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error building config: %s\n", err)
		os.Exit(2)
	}

	// 3. Diff + lint, or just lint if using --target-alter.
	if len(cmd.TargetAlter) > 0 {
		// Imperative path: ALTER statements provided directly.
		// There is no declarative target, so we lint the provided changes
		// against the source schema.
		changes, err := loadAlterChanges(cmd.TargetAlter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading changes: %s\n", err)
			os.Exit(2)
		}
		config.LintOnlyChanges = true
		violations, err := RunLinters(source, changes, config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running linters: %s\n", err)
			os.Exit(2)
		}
		printViolationsAsSQL(violations)
		if len(violations) > 0 && len(changes) > 0 {
			fmt.Println()
		}
		printDiff(changes)
		if HasErrors(violations) {
			os.Exit(1)
		}
	} else {
		// Declarative path: diff source against target, then lint.
		target, err := cmd.loadTarget(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading target schema: %s\n", err)
			os.Exit(2)
		}
		currentSchemas, err := createTablesToTableSchemas(source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting source schema: %s\n", err)
			os.Exit(2)
		}
		targetSchemas, err := createTablesToTableSchemas(target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting target schema: %s\n", err)
			os.Exit(2)
		}
		plan, err := PlanChanges(currentSchemas, targetSchemas, nil, &config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error planning changes: %s\n", err)
			os.Exit(2)
		}
		printPlan(plan)
		if plan.HasErrors() {
			os.Exit(1)
		}
	}
	return nil
}

// loadTarget loads the target schema from a directory or DSN.
func (cmd *DiffCmd) loadTarget(ctx context.Context) ([]*statement.CreateTable, error) {
	if cmd.TargetDir != "" {
		return LoadSchemaFromDir(cmd.TargetDir)
	}
	return LoadSchemaFromDSN(ctx, cmd.TargetDSN)
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

// createTablesToTableSchemas converts parsed CreateTable objects to
// table.TableSchema values for use with PlanChanges.
func createTablesToTableSchemas(tables []*statement.CreateTable) ([]table.TableSchema, error) {
	schemas := make([]table.TableSchema, len(tables))
	for i, ct := range tables {
		ts, err := ct.ToTableSchema()
		if err != nil {
			return nil, err
		}
		schemas[i] = ts
	}
	return schemas, nil
}

// printPlan prints a Plan as valid SQL: violations as comments, then DDL.
// The statement order from PlanChanges is preserved (CREATE/ALTER before DROP,
// sorted by table name within each group).
func printPlan(plan *Plan) {
	// Collect all violations across changes for the comment header.
	var hasViolations bool
	for _, ch := range plan.Changes {
		for _, e := range ch.Errors {
			fmt.Printf("-- %s\n", e)
			hasViolations = true
		}
		for _, w := range ch.Warnings {
			fmt.Printf("-- %s\n", w)
			hasViolations = true
		}
		for _, info := range ch.Infos {
			fmt.Printf("-- %s\n", info)
			hasViolations = true
		}
	}

	if hasViolations && plan.HasChanges() {
		fmt.Println()
	}
	if !plan.HasChanges() {
		fmt.Println("-- No schema differences found.")
		return
	}

	for _, ch := range plan.Changes {
		fmt.Printf("%s\n", terminatedStmt(ch.Statement))
	}
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
