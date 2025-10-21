package migration

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

var linterConfigRE = regexp.MustCompile(`^([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)=(.+)$`)

func (r *Runner) lint(ctx context.Context) error {
	var createTables []*statement.CreateTable
	var alterTables []*statement.AbstractStatement
	config := lint.Config{}
	if config.Enabled == nil {
		config.Enabled = make(map[string]bool)
	}
	if config.Settings == nil {
		config.Settings = make(map[string]map[string]string)
	}

	// Set default config for invisible_index_before_drop linter
	// TODO: Put these defaults in a more sensible place
	config.Settings["invisible_index_before_drop"] = map[string]string{"raiseError": "true"}

	for _, linterName := range r.migration.EnableExperimentalLinters {
		if linterName[0] == '-' {
			config.Enabled[linterName[1:]] = false
		} else {
			config.Enabled[linterName] = true
		}
	}

	for _, cfg := range r.migration.ExperimentalLinterConfig {
		matches := linterConfigRE.FindStringSubmatch(cfg)
		if len(matches) != 4 {
			return fmt.Errorf("invalid linter config: %s", cfg)
		}
		linterName := matches[1]
		key := matches[2]
		value := matches[3]

		if _, ok := config.Settings[linterName]; !ok {
			config.Settings[linterName] = make(map[string]string)
		}
		config.Settings[linterName][key] = value
	}

	if err := printLinters(config); err != nil {
		return err
	}

	for _, change := range r.changes {
		// Collect ALTER TABLE statements and their corresponding CREATE TABLEs
		if change.stmt.IsAlterTable() {
			alterTables = append(alterTables, change.stmt)

			ct, err := r.getCreateTable(ctx, change.stmt.Schema, change.stmt.Table)
			if err != nil {
				return err
			}
			createTables = append(createTables, ct)
		}

		// Collect standalone CREATE TABLE statements
		if ctAST, ok := (*change.stmt.StmtNode).(*ast.CreateTableStmt); ok {
			ct, err := statement.ParseCreateTableAST(ctAST)
			if err != nil {
				return err
			}
			createTables = append(createTables, ct)
		}
	}

	var errs []error

	violations, err := lint.RunLinters(createTables, alterTables, config)
	if err != nil {
		errs = append(errs, err)
	}

	for _, v := range violations {
		if v.Severity == lint.SeverityError {
			errs = append(errs, errors.New(v.String()))
		}
		fmt.Println(v)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Runner) getCreateTable(ctx context.Context, db string, tbl string) (*statement.CreateTable, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s`", db, tbl)
	row := r.db.QueryRowContext(ctx, sql)
	var createTable string
	if err := row.Scan(&tbl, &createTable); err != nil {
		return nil, err
	}
	stmt, err := statement.ParseCreateTable(createTable)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func printLinters(config lint.Config) error {
	fmt.Printf("Linting is enabled with the following linters:\n")
	linters := lint.List()
	for _, linterName := range linters {
		l, err := lint.Get(linterName)
		// Skip linters that are not enabled
		if !config.IsEnabled(linterName) {
			continue
		}

		if err != nil {
			return err
		}
		fmt.Printf(" + %s", l)
		if cfg, ok := config.Settings[linterName]; ok {
			fmt.Print(" (with config")
			for k, v := range cfg {
				fmt.Printf(" %s=%s", k, v)
			}
			fmt.Print(") ")
		}
		fmt.Println()
	}
	return nil
}
