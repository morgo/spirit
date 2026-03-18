package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/table"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// DeclarativeToImperative compares current and desired schemas and returns the
// imperative DDL statements (ALTER, CREATE, DROP) needed to transform current
// into desired.
//
// This is the core of declarative schema management: given two sets of table
// definitions, compute the minimal set of changes. It is used by spirit's diff
// subcommand, strata, and GAP.
//
// If opts is nil, NewDiffOptions() defaults are used for table diffs.
func DeclarativeToImperative(current, desired []table.TableSchema, opts *DiffOptions) ([]*AbstractStatement, error) {
	currentMap := make(map[string]table.TableSchema, len(current))
	desiredMap := make(map[string]table.TableSchema, len(desired))
	for _, t := range current {
		currentMap[t.Name] = t
	}
	for _, t := range desired {
		desiredMap[t.Name] = t
	}

	var changes []*AbstractStatement

	// Tables in desired: create if new, diff if existing.
	for name, desiredTable := range desiredMap {
		existingTable, exists := currentMap[name]
		if !exists {
			// New table — emit CREATE TABLE.
			stmts, err := New(desiredTable.Schema)
			if err != nil {
				return nil, fmt.Errorf("failed to parse CREATE TABLE for new table %q: %w", name, err)
			}
			changes = append(changes, stmts...)
			continue
		}

		// Both exist — compute ALTER TABLE diff.
		a, err := ParseCreateTable(existingTable.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse current schema for table %q: %w", name, err)
		}
		b, err := ParseCreateTable(desiredTable.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse desired schema for table %q: %w", name, err)
		}
		diffs, err := a.Diff(b, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to diff table %q: %w", name, err)
		}
		changes = append(changes, diffs...)
	}

	// Tables in current but not in desired — emit DROP TABLE.
	for name := range currentMap {
		if _, exists := desiredMap[name]; !exists {
			stmts, err := New(fmt.Sprintf("DROP TABLE `%s`", name))
			if err != nil {
				return nil, fmt.Errorf("failed to parse DROP TABLE for %q: %w", name, err)
			}
			changes = append(changes, stmts...)
		}
	}

	return changes, nil
}

// ToTableSchema converts a parsed CreateTable back to a table.TableSchema
// by restoring the AST to SQL. This is useful when callers have already parsed
// schemas (e.g. for linting) but need to pass them to DeclarativeToImperative.
func (ct *CreateTable) ToTableSchema() table.TableSchema {
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		// This should never fail for a valid CreateTable that was parsed successfully.
		// If it does, return the table name with an empty schema so the error surfaces
		// downstream during parsing.
		return table.TableSchema{Name: ct.TableName}
	}
	return table.TableSchema{
		Name:   ct.TableName,
		Schema: sb.String(),
	}
}
