package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
)

// Experimental support for the foreign keys of the table being altered
// (--enable-experimental-foreign-keys, MySQL 9.7 and later; see
// check.MinForeignKeyVersion). A table that other tables' foreign keys
// reference is still refused, by the hasforeignkeys check.
//
// CREATE TABLE .. LIKE does not copy foreign keys, so copyForeignKeys adds them
// to the _new table. Their names are unique per schema, so the copies are named
// by utils.NewForeignKeyName. The cutover's RENAME TABLE gives a generated name
// (<table>_ibfk_<n>) back to its copy; restoreForeignKeyNames renames the
// others back once the original table has been dropped.

// copyForeignKeys adds the foreign keys of table sourceName to its new table
// newName, under the names utils.NewForeignKeyName gives them. oldName is the
// name the cutover renames the source table to, which also renames its
// generated foreign keys.
//
// Adding a named foreign key replaces the index MySQL created for the original
// one with an index named after the constraint. The indexes are renamed back
// afterwards, so the new table's indexes are the source table's.
func copyForeignKeys(ctx context.Context, db *sql.DB, logger *slog.Logger, sourceName, newName, oldName string) error {
	source, err := tableDefinition(ctx, db, sourceName)
	if err != nil {
		return err
	}
	var clauses []string
	for _, fk := range source.GetConstraints() {
		if fk.Type != "FOREIGN KEY" {
			continue
		}
		name := utils.NewForeignKeyName(sourceName, fk.Name)
		for _, n := range []string{name, utils.RenamedForeignKeyName(fk.Name, sourceName, oldName)} {
			if len(n) > utils.MaxTableNameLength {
				return fmt.Errorf("foreign key %s of table %s cannot be copied: the migration would have to name it %s, which is longer than MySQL's %d character limit",
					fk.Name, sourceName, n, utils.MaxTableNameLength)
			}
		}
		clause, err := restoreForeignKey(fk, name)
		if err != nil {
			return err
		}
		clauses = append(clauses, "ADD "+clause)
	}
	if len(clauses) == 0 {
		return nil
	}
	// The new table is empty, so the checks cost nothing here. Leaving them on
	// keeps MySQL validating the definitions, as it does for the user's ALTER
	// that follows.
	if err := dbconn.Exec(ctx, db, "ALTER TABLE %n %r", newName, sqlescape.RawSQL(strings.Join(clauses, ", "))); err != nil {
		return fmt.Errorf("could not copy the foreign keys of %s to %s: %w", sourceName, newName, err)
	}
	logger.Info("copied foreign keys to the new table", "table", newName, "foreign-keys", len(clauses))
	return restoreIndexNames(ctx, db, source, newName)
}

// restoreIndexNames renames each index of newName that copyForeignKeys
// replaced back to the name it has on source: an index source does not have,
// on the same columns as an index of source that newName no longer has.
func restoreIndexNames(ctx context.Context, db *sql.DB, source *statement.CreateTable, newName string) error {
	newTable, err := tableDefinition(ctx, db, newName)
	if err != nil {
		return err
	}
	has := func(def *statement.CreateTable, name string) bool {
		return slices.ContainsFunc(def.GetIndexes(), func(i statement.Index) bool { return strings.EqualFold(i.Name, name) })
	}
	var clauses []string
	for _, idx := range source.GetIndexes() {
		if has(newTable, idx.Name) {
			continue
		}
		i := slices.IndexFunc(newTable.GetIndexes(), func(c statement.Index) bool {
			return !has(source, c.Name) && c.Type == idx.Type && reflect.DeepEqual(c.ColumnList, idx.ColumnList)
		})
		if i < 0 {
			return fmt.Errorf("index %s of %s is missing from %s after its foreign keys were copied", idx.Name, source.GetTableName(), newName)
		}
		clauses = append(clauses, sqlescape.MustEscapeSQL("RENAME INDEX %n TO %n", newTable.GetIndexes()[i].Name, idx.Name))
	}
	if len(clauses) == 0 {
		return nil
	}
	return dbconn.Exec(ctx, db, "ALTER TABLE %n %r", newName, sqlescape.RawSQL(strings.Join(clauses, ", ")))
}

// foreignKeyRenames maps the lower-cased name of each foreign key of table
// tableName to the name of its copy on the new table.
func foreignKeyRenames(ctx context.Context, db *sql.DB, tableName string) (map[string]string, error) {
	source, err := tableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}
	renames := make(map[string]string)
	for _, fk := range source.GetConstraints() {
		if fk.Type == "FOREIGN KEY" {
			renames[strings.ToLower(fk.Name)] = utils.NewForeignKeyName(tableName, fk.Name)
		}
	}
	return renames, nil
}

// restoreForeignKeyNames renames the foreign keys of tableName, which the
// cutover has just put in place, back to the names of the foreign keys they
// copy. It must only run once the original table has been dropped, as that
// still holds the names.
//
// The names to restore are read off the table rather than remembered from
// setup, so a migration resumed before its cutover restores them too. Only the
// run that did the cutover calls it: a run that stops between the cutover and
// the rename leaves the copies' names, and the next run cannot tell them from
// foreign keys a user named _<name>_new. Every foreign key of the
// table is a copy (the ALTER cannot add one), so a name of the form _<name>_new
// is the copy of <name>; one with the table's generated prefix was renamed by
// the cutover already, and one with the new table's generated prefix should
// have been.
//
// The checks are off for the ALTER, so it drops and re-adds each foreign key
// as a metadata change rather than copying the table, without checking the
// rows: the same foreign key held them a moment earlier, under the other name.
// It returns the names it renamed from. With apply false, it only returns the
// names it would rename from.
func restoreForeignKeyNames(ctx context.Context, db *sql.DB, tableName string, apply bool) ([]string, error) {
	def, err := tableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}
	newName := utils.NewTableName(tableName)
	var clauses, names []string
	for _, fk := range def.GetConstraints() {
		if fk.Type != "FOREIGN KEY" {
			continue
		}
		original := utils.RenamedForeignKeyName(fk.Name, newName, tableName)
		if original == fk.Name {
			// Not a generated name the cutover failed to rename: the copy of
			// a named foreign key, or a generated name the cutover renamed.
			trimmed, hasPrefix := strings.CutPrefix(fk.Name, "_")
			name, hasSuffix := strings.CutSuffix(trimmed, "_new")
			if !hasPrefix || !hasSuffix || name == "" || strings.HasPrefix(fk.Name, tableName+"_ibfk_") {
				continue
			}
			original = name
		}
		clause, err := restoreForeignKey(fk, original)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, sqlescape.MustEscapeSQL("DROP FOREIGN KEY %n", fk.Name), "ADD "+clause)
		names = append(names, fk.Name)
	}
	if len(clauses) == 0 || !apply {
		return names, nil
	}
	if err := dbconn.ExecWithoutForeignKeyChecks(ctx, db, "ALTER TABLE %n %r, ALGORITHM=INPLACE, LOCK=NONE",
		tableName, sqlescape.RawSQL(strings.Join(clauses, ", "))); err != nil {
		return nil, err
	}
	return names, nil
}

// restoreForeignKey returns the definition of foreign key fk, as an ALTER
// TABLE .. ADD takes it, named name.
func restoreForeignKey(fk statement.Constraint, name string) (string, error) {
	if fk.Raw == nil {
		return "", fmt.Errorf("foreign key %s has no parsed definition", fk.Name)
	}
	raw := *fk.Raw
	raw.Name = name
	var sb strings.Builder
	if err := raw.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", fmt.Errorf("could not restore foreign key %s: %w", fk.Name, err)
	}
	return sb.String(), nil
}
