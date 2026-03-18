package statement

import (
	"fmt"
	"sort"
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
// The returned statements are ordered by table name, with CREATE/ALTER
// statements before DROP statements.
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

	// Collect sorted table names for deterministic output.
	desiredNames := make([]string, 0, len(desiredMap))
	for name := range desiredMap {
		desiredNames = append(desiredNames, name)
	}
	sort.Strings(desiredNames)

	var changes []*AbstractStatement
	var drops []*AbstractStatement

	// Tables in desired: create if new, diff if existing.
	for _, name := range desiredNames {
		desiredTable := desiredMap[name]
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
	dropNames := make([]string, 0)
	for name := range currentMap {
		if _, exists := desiredMap[name]; !exists {
			dropNames = append(dropNames, name)
		}
	}
	sort.Strings(dropNames)

	for _, name := range dropNames {
		stmts, err := New(fmt.Sprintf("DROP TABLE `%s`", name))
		if err != nil {
			return nil, fmt.Errorf("failed to parse DROP TABLE for %q: %w", name, err)
		}
		drops = append(drops, stmts...)
	}

	// CREATE/ALTER first, then DROP.
	changes = append(changes, drops...)
	return changes, nil
}

// ToTableSchema converts a parsed CreateTable back to a table.TableSchema
// by restoring the AST to SQL. This is useful when callers have already parsed
// schemas (e.g. for linting) but need to pass them to DeclarativeToImperative.
func (ct *CreateTable) ToTableSchema() (table.TableSchema, error) {
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return table.TableSchema{}, fmt.Errorf("failed to restore CREATE TABLE for %q: %w", ct.TableName, err)
	}
	return table.TableSchema{
		Name:   ct.TableName,
		Schema: sb.String(),
	}, nil
}
