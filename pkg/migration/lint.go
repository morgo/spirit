package migration

import (
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
)

func (r *Runner) lint() error {
	var createTables []*statement.CreateTable
	var alterTables []*statement.AbstractStatement
	config := lint.Config{}
	if config.Settings == nil {
		config.Settings = make(map[string]map[string]string)
	}
	config.Settings["invisible_index_before_drop"] = map[string]string{"raiseError": "true"}

	for _, change := range r.changes {
		if change.stmt.IsAlterTable() {
			alterTables = append(alterTables, change.stmt)
		}

		ct, err := r.getCreateTable(change.stmt.Schema, change.stmt.Table)
		if err != nil {
			return err
		}
		createTables = append(createTables, ct)
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

func (r *Runner) getCreateTable(db string, tbl string) (*statement.CreateTable, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s`", db, tbl)
	row := r.db.QueryRow(sql)
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
