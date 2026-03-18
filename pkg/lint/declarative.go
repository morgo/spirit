package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

// PlannedChange represents a single DDL statement produced by declarative
// schema diffing, along with any lint violations associated with it.
type PlannedChange struct {
	// Statement is the semicolon-terminated DDL (ALTER, CREATE, or DROP TABLE).
	Statement string

	// TableName is the table affected by this change.
	TableName string

	// Warnings contains lint violations with WARNING severity for this table.
	// Linters operate at the table level. When a diff produces multiple
	// statements for the same table, violations are attached only to the
	// last statement to avoid duplication.
	Warnings []string

	// Errors contains lint violations with ERROR severity for this table.
	// See Warnings for multi-statement behavior.
	Errors []string

	// Infos contains lint violations with INFO severity for this table.
	// These are suggestions or style preferences that callers may choose to ignore.
	// See Warnings for multi-statement behavior.
	Infos []string
}

// Plan holds the complete result of a declarative-to-imperative diff with linting.
type Plan struct {
	// Changes is the ordered list of DDL changes with per-statement lint results.
	Changes []PlannedChange
}

// HasChanges returns true if the plan contains any DDL statements.
func (p *Plan) HasChanges() bool {
	return len(p.Changes) > 0
}

// HasErrors returns true if any change has lint errors.
func (p *Plan) HasErrors() bool {
	for i := range p.Changes {
		if len(p.Changes[i].Errors) > 0 {
			return true
		}
	}
	return false
}

// HasWarnings returns true if any change has lint warnings.
func (p *Plan) HasWarnings() bool {
	for i := range p.Changes {
		if len(p.Changes[i].Warnings) > 0 {
			return true
		}
	}
	return false
}

// HasInfos returns true if any change has lint infos.
func (p *Plan) HasInfos() bool {
	for i := range p.Changes {
		if len(p.Changes[i].Infos) > 0 {
			return true
		}
	}
	return false
}

// Statements returns just the DDL strings from all changes.
func (p *Plan) Statements() []string {
	stmts := make([]string, len(p.Changes))
	for i, ch := range p.Changes {
		stmts[i] = ch.Statement
	}
	return stmts
}

// PlanChanges computes the imperative DDL statements needed to transform the
// current schema into the desired schema, and lints each statement against the
// current schema. This combines statement.DeclarativeToImperative with RunLinters
// into a single call, returning per-statement lint results.
//
// LintOnlyChanges is always set to true regardless of the provided lintConfig,
// since PlanChanges only produces lint results for tables that have changes.
//
// Parameters:
//   - current: the current table schemas (CREATE TABLE DDL)
//   - desired: the desired table schemas (CREATE TABLE DDL)
//   - diffOpts: options for the diff (nil uses defaults)
//   - lintConfig: configuration for linting (nil uses defaults; LintOnlyChanges is always overridden to true)
func PlanChanges(current, desired []table.TableSchema, diffOpts *statement.DiffOptions, lintConfig *Config) (*Plan, error) {
	// 1. Compute the diff.
	changes, err := statement.DeclarativeToImperative(current, desired, diffOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to compute schema diff: %w", err)
	}

	// 2. Parse the current schemas for linting.
	var createTables []*statement.CreateTable
	for _, t := range current {
		ct, err := statement.ParseCreateTable(t.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse current schema for table %q: %w", t.Name, err)
		}
		createTables = append(createTables, ct)
	}

	// 3. Run linters.
	cfg := Config{LintOnlyChanges: true}
	if lintConfig != nil {
		cfg = *lintConfig
		cfg.LintOnlyChanges = true
	}
	violations, err := RunLinters(createTables, changes, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to run linters: %w", err)
	}

	// 4. Build per-table violation map.
	// Linters operate at the table level, so violations are grouped by table
	// name. In step 5, they are attached only to the last statement for each
	// table to avoid duplication when a diff produces multiple statements
	// (e.g. partition type changes).
	//
	// Violations are sorted first to ensure deterministic output, since
	// RunLinters iterates over a map of registered linters.
	type tableViolations struct {
		warnings []string
		errors   []string
		infos    []string
	}
	violationsByTable := make(map[string]*tableViolations)
	for _, v := range sortViolations(violations) {
		tableName := ""
		if v.Location != nil {
			tableName = v.Location.Table
		}
		tv, ok := violationsByTable[tableName]
		if !ok {
			tv = &tableViolations{}
			violationsByTable[tableName] = tv
		}
		switch v.Severity {
		case SeverityError:
			tv.errors = append(tv.errors, v.String())
		case SeverityWarning:
			tv.warnings = append(tv.warnings, v.String())
		case SeverityInfo:
			tv.infos = append(tv.infos, v.String())
		}
	}

	// 5. Build the plan, attaching violations to the last statement per table.
	// A diff may produce multiple statements for the same table (e.g. partition
	// type changes require REMOVE PARTITIONING then PARTITION BY). Violations
	// are attached only to the last statement for each table so they are not
	// duplicated.
	plan := &Plan{}
	lastIndexByTable := make(map[string]int)
	for i, ch := range changes {
		plan.Changes = append(plan.Changes, PlannedChange{
			Statement: terminatedStmt(ch.Statement),
			TableName: ch.Table,
		})
		lastIndexByTable[ch.Table] = i
	}
	for tableName, tv := range violationsByTable {
		idx, ok := lastIndexByTable[tableName]
		if !ok {
			continue
		}
		plan.Changes[idx].Warnings = tv.warnings
		plan.Changes[idx].Errors = tv.errors
		plan.Changes[idx].Infos = tv.infos
	}

	return plan, nil
}

// terminatedStmt ensures a SQL statement ends with a semicolon.
func terminatedStmt(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	if len(stmt) == 0 {
		return stmt
	}
	if stmt[len(stmt)-1] != ';' {
		return stmt + ";"
	}
	return stmt
}
