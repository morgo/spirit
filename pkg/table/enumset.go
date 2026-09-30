package table

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// ENUM/SET binlog decoding.
//
// The go-mysql binlog reader returns ENUM values as int64 ordinals
// (1-indexed) and SET values as int64 bitmasks. To replay those values
// onto a target column that has been migrated to a string type (e.g.
// VARCHAR), we need the original string elements. They are not carried
// in the binlog stream, so we recover them by parsing the column's
// `column_type` text from information_schema, which TableInfo already
// caches in columnsMySQLTps (see utils.ParseEnumSetElements).
//
// That text is not always the members MySQL stores. The data dictionary
// keeps column_type as utf8mb3, so each member character outside utf8mb3 (a
// 4-byte UTF-8 character such as an emoji) is reported as '?', both there and
// in SHOW CREATE TABLE, whatever the connection charset. The rows hold the
// real member. A member list that contains a '?' is therefore read back from
// MySQL itself with readStoredEnumSetMembers.

// decodeEnumOrdinal converts a 1-indexed ENUM ordinal into the matching
// element string. Ordinal 0 is MySQL's empty-string sentinel (”) for an
// invalid value and is preserved as "". Out-of-range ordinals are an
// error rather than a silent miscoding.
func decodeEnumOrdinal(ordinal int64, elements []string) (string, error) {
	if ordinal == 0 {
		return "", nil
	}
	if ordinal < 0 || int(ordinal) > len(elements) {
		return "", fmt.Errorf("ENUM ordinal %d out of range for %d elements", ordinal, len(elements))
	}
	return elements[ordinal-1], nil
}

// decodeSetBitmask converts a SET bitmask into a comma-joined string of
// the elements whose bits are set. Bit i (0-indexed) corresponds to
// elements[i]. Bits set above len(elements) are an error.
//
// The bitmask arrives from the go-mysql binlog reader as int64 because
// that is what its decodeValue() path returns, but MySQL SET supports
// up to 64 members — a value with bit 63 set (or all 64 bits set,
// which surfaces as -1) is valid. We reinterpret the int64 as uint64
// to walk the bits; out-of-range bits are caught by the
// i >= len(elements) guard rather than by a sign check.
func decodeSetBitmask(bitmask int64, elements []string) (string, error) {
	if bitmask == 0 {
		return "", nil
	}
	bits := uint64(bitmask)
	maxBits := len(elements)
	var parts []string
	for i := range 64 {
		if bits&(uint64(1)<<i) == 0 {
			continue
		}
		if i >= maxBits {
			return "", fmt.Errorf("SET bitmask bit %d set but only %d elements defined", i, maxBits)
		}
		parts = append(parts, elements[i])
	}
	return strings.Join(parts, ","), nil
}

// enumSetProbeTable is the name of the temporary table
// readStoredEnumSetMembers creates. A temporary table is visible only to the
// session that creates it, so it cannot collide with another session's, and
// it shadows a real table of the same name only within that session.
const enumSetProbeTable = "_spirit_enumset_probe"

// readStoredEnumSetMembers returns the members MySQL stores for the ENUM or
// SET column column of table tableName (in the connection's schema), which
// information_schema reports with count members.
//
// No information_schema table or SHOW statement reports a member character
// outside utf8mb3 (see the package comment above), and the data dictionary
// table that stores the members, mysql.column_type_elements, cannot be read
// (error 3554). A table created from a SELECT of the column takes its stored
// definition, though, so the members are read by storing each ordinal (ENUM)
// or single-bit mask (SET) in such a temporary table and reading the values
// back. That needs the CREATE TEMPORARY TABLES privilege, and works on a
// read-only server. No row of tableName is read.
//
// The probe runs on a connection of its own, which is then closed rather
// than returned to db's pool, so that the temporary table ends with it.
func readStoredEnumSetMembers(ctx context.Context, db *sql.DB, tableName, column string, isSet bool, count int) (members []string, err error) {
	if isSet && count > 64 {
		// Each SET member is one bit of a 64-bit value.
		return nil, fmt.Errorf("SET column %q reports %d members, more than the 64 MySQL allows", column, count)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		// ErrBadConn through Raw instructs database/sql to close the
		// underlying connection instead of returning it to the pool.
		rawErr := conn.Raw(func(any) error { return driver.ErrBadConn })
		if errors.Is(rawErr, driver.ErrBadConn) || errors.Is(rawErr, sql.ErrConnDone) {
			rawErr = nil
		}
		closeErr := conn.Close()
		if errors.Is(closeErr, sql.ErrConnDone) {
			closeErr = nil
		}
		err = errors.Join(err, rawErr, closeErr)
	}()
	quotedColumn := sqlescape.EscapeIdentifier(column)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s SELECT %s FROM %s LIMIT 0",
		sqlescape.EscapeIdentifier(enumSetProbeTable), quotedColumn, sqlescape.EscapeIdentifier(tableName))); err != nil {
		return nil, err
	}
	values := make([]string, count)
	for i := range count {
		if isSet {
			values[i] = "(" + strconv.FormatUint(uint64(1)<<i, 10) + ")"
		} else {
			values[i] = "(" + strconv.Itoa(i+1) + ")"
		}
	}
	if count > 0 {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			sqlescape.EscapeIdentifier(enumSetProbeTable), quotedColumn, strings.Join(values, ","))); err != nil {
			return nil, err
		}
	}
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s+0",
		quotedColumn, sqlescape.EscapeIdentifier(enumSetProbeTable), quotedColumn))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(members) != count {
		return nil, fmt.Errorf("read %d members of column %q back from MySQL, expected %d", len(members), column, count)
	}
	return members, nil
}
