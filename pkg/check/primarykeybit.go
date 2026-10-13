package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	register(check{name: "primarykeybit", run: primaryKeyBitCheck, phases: PhaseStatement | PhasePostSetup, runtimes: Migration})
}

// primaryKeyBitCheck refuses every ALTER against a table whose primary key
// has a BIT column, and any ALTER that leaves a BIT in the primary key: see
// table.TableInfo.BitPrimaryKeyError.
//
// At statement phase it reads the table's current definition and the
// statement: a BIT already in the key, or a MODIFY or CHANGE of a key column
// to a BIT. The migration runner runs the statement-phase checks before it
// attempts MySQL's native DDL, so the refusal holds for every ALTER shape on
// every server, including ones the native DDL could complete. The verdict
// needs the table's current definition; a statement-phase caller that does not
// supply Resources.Change.Table gets no verdict rather than a guess.
//
// At post-setup it also reads the new table, which MySQL has already altered,
// so a BIT that reaches the key by a route the statement does not spell out as
// a MODIFY or CHANGE of a key column is refused before any row is copied.
// Changing the key's columns or types always requires a table copy, which the
// native DDL attempt (INSTANT, then a safe INPLACE subset) never performs, so
// the native attempt cannot complete such a statement ahead of this phase.
func primaryKeyBitCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// Post-setup runs inside a migration, which loads both tables first. A
	// missing one there means the check would not run at all, so fail
	// rather than pass quietly.
	if r.phase&PhasePostSetup != 0 && (r.Change.Table == nil || r.Change.NewTable == nil) {
		return errors.New("check primarykeybit cannot run: the table and the new table were not loaded")
	}
	if r.Change.Table == nil {
		logger.Debug("skipping check: no table metadata supplied, cannot tell the types of the primary key columns",
			"check", "primarykeybit")
		return nil
	}
	if err := r.Change.Table.BitPrimaryKeyError(); err != nil {
		return err
	}
	if r.Change.Statement != nil && r.Change.Statement.StmtNode != nil {
		for _, col := range findModifiedColumns(*r.Change.Statement.StmtNode) {
			if col.ColDef.Tp.GetType() != mysql.TypeBit {
				continue
			}
			for _, keyCol := range r.Change.Table.KeyColumns {
				if strings.EqualFold(keyCol, col.LookupName) {
					return fmt.Errorf("changing primary key column %q of table %q to a BIT is not supported", keyCol, r.Change.Table.TableName)
				}
			}
		}
	}
	if r.Change.NewTable != nil {
		if err := r.Change.NewTable.BitPrimaryKeyError(); err != nil {
			return fmt.Errorf("altering table %q so that its primary key includes a BIT is not supported: %w", r.Change.Table.TableName, err)
		}
	}
	return nil
}
