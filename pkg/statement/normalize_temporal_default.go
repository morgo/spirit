package statement

import (
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(temporalDefaultNormalizer{}) }

// temporalDefaultNormalizer rewrites a literal DEFAULT on a DATE, DATETIME,
// TIMESTAMP or TIME column to the value MySQL stores, in the text SHOW CREATE
// TABLE reports: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS' or '[-]HH:MM:SS', each
// with exactly the column's fractional digits. MySQL accepts a temporal
// literal in many spellings — a date without a time, one-digit fields, a
// two-digit year, any punctuation between fields, a compact digit string, a
// number, a fraction longer than the column keeps — and reports only the
// stored value, so a column declared `datetime DEFAULT '2020-1-1'` reads back
// as `DEFAULT '2020-01-01 00:00:00'`. Without the rule the declared literal
// diffs against the live one forever: every run emits a MODIFY COLUMN that
// stores the same value again. Each reading below was taken from MySQL 8.0.43
// with the default strict sql_mode; the reader itself is [utils.Temporal].
//
//	datetime DEFAULT '2020-1-1'                -> '2020-01-01 00:00:00'
//	datetime DEFAULT '20-01-01T10:00'          -> '2020-01-01 10:00:00'
//	datetime DEFAULT '2020.01.01 10.00.00'     -> '2020-01-01 10:00:00'
//	datetime DEFAULT 20200101                  -> '2020-01-01 00:00:00'
//	datetime DEFAULT 101                       -> '2000-01-01 00:00:00'
//	datetime(3) DEFAULT '2020-01-01 10:00:00'  -> '2020-01-01 10:00:00.000'
//	datetime(3) DEFAULT '... 10:00:00.1235'    -> '2020-01-01 10:00:00.124'
//	datetime DEFAULT '2020-01-01 23:59:59.9'   -> '2020-01-02 00:00:00'
//	date DEFAULT '2020-01-01 23:59:59.9'       -> '2020-01-02'
//	date DEFAULT 20200101                      -> '2020-01-01'
//	time DEFAULT '1:2'                         -> '01:02:00'
//	time DEFAULT '1 2:3:4.5'                   -> '26:03:05'
//	time DEFAULT 100                           -> '00:01:00'
//	time(1) DEFAULT 1.55                       -> '00:00:01.6'
//	time DEFAULT '-0:00:00.4'                  -> '00:00:00'
//
// A fraction rounds half up to the column's precision, in two steps the way
// MySQL does it: the digit past the microseconds rounds them first. A DATETIME
// string rounds from the seventh digit and a TIME string from the last digit
// written, so '10:00:00.1234564999' is '.123456' on a datetime(6) and
// '.123457' on a time(6). The carry runs through the seconds, minutes, hours
// and date.
//
// The result is recorded as a [DefaultKindString], the form SHOW CREATE TABLE
// reports, so a declared number compares equal to the live string.
//
// Left alone, so that the diff keeps emitting the literal as written:
//
//   - a value MySQL rejects (an invalid date, a zero month or day, a field
//     out of range, a carry past 9999-12-31 or 838:59:59), so the MODIFY
//     fails the way it would have anyway.
//   - a literal with a time-zone suffix ('2020-01-01 10:00:00+00:00'), which
//     MySQL converts to the session time zone: no fixed text reproduces it.
//   - a float literal (`1e2`), which MySQL reads through a double, rounding a
//     long fraction differently from the decimal reading.
//   - the spellings MySQL reads by rules not worth reproducing: a compact
//     digit string of a width other than 6, 8, 12 or 14 ('10101' is
//     2010-10-01), a thirteen-digit number (a year 100-999 datetime), a TIME
//     string that starts with a colon or that MySQL reads as a DATETIME first
//     (twelve or more digits, or a date), and a number MySQL reads as a
//     DATETIME on a TIME column.
//   - a hex or bit literal, TRUE/FALSE, an expression default and NULL.
//
// TIMESTAMP has one more wrinkle this rule cannot remove: MySQL converts a
// TIMESTAMP default to UTC with the session time zone of the CREATE and
// reports it in the session time zone of the SHOW CREATE TABLE, so the live
// text depends on both sessions. The rule canonicalizes the spelling only.
type temporalDefaultNormalizer struct{}

func (temporalDefaultNormalizer) Name() string { return "temporal-default" }

func (temporalDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr {
			continue
		}
		if c.DefaultKind != DefaultKindString && c.DefaultKind != DefaultKindNumber {
			continue
		}
		stored, ok := temporalDefaultText(c, strings.ToLower(c.Type))
		if !ok {
			continue
		}
		c.Default, c.DefaultKind = &stored, DefaultKindString
	}
	return ct
}

// temporalDefaultText returns the text SHOW CREATE TABLE reports for a
// temporal column's literal default, or false where the default is not one
// this rule converts. See [temporalDefaultNormalizer].
func temporalDefaultText(c *Column, typ string) (string, bool) {
	text := *c.Default
	number := c.DefaultKind == DefaultKindNumber
	if number && strings.ContainsAny(text, "eE") {
		return "", false // a float literal is read through a double
	}
	fsp := 0
	if c.Length != nil {
		fsp = *c.Length
	}
	switch typ {
	case "date", "datetime", "timestamp":
		var value utils.Temporal
		var ok bool
		if number {
			value, ok = utils.ParseDateTimeNumber(text)
		} else {
			value, ok = utils.ParseDateTimeString(text)
		}
		if !ok {
			return "", false
		}
		if typ == "date" {
			// A DATE keeps the date after the time part has rounded away.
			if value, ok = value.RoundDateTime(0); !ok {
				return "", false
			}
			return value.DateString(), true
		}
		if value, ok = value.RoundDateTime(fsp); !ok {
			return "", false
		}
		return value.DateTimeString(fsp), true
	case "time":
		var value utils.Temporal
		var ok bool
		if number {
			value, ok = utils.ParseTimeNumber(text)
		} else {
			value, ok = utils.ParseTimeString(text)
		}
		if !ok {
			return "", false
		}
		if value, ok = value.RoundTime(fsp); !ok {
			return "", false
		}
		return value.TimeString(fsp), true
	}
	return "", false
}
