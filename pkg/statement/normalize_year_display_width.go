package statement

import "strings"

func init() { registerNormalizer(yearDisplayWidthNormalizer{}) }

// yearDisplayWidthNormalizer drops the display width from a YEAR column.
// YEAR(4) is the only width MySQL 8.0 still accepts (every other width is
// rejected, by the server with ERROR 1818 and by the parser; clearing Length
// unconditionally relies on that, and TestYearDisplayWidthOtherWidthsRejected
// pins it), and it is deprecated: MySQL stores the column
// as a plain `year`, so SHOW CREATE TABLE never reports a width. Verified
// against MySQL 8.0.43:
//
//	year(4)              -> year
//	year(4) DEFAULT 2024 -> year DEFAULT '2024'
//
// The parser records the width in Column.Length for year(4) and leaves it nil
// for a width-less `year`. Without this rule a schema that writes year(4)
// diffs against the live `year`, and the emitted `MODIFY COLUMN ... year(4)`
// is stored as `year` again, so the diff is re-emitted on every run.
type yearDisplayWidthNormalizer struct{}

func (yearDisplayWidthNormalizer) Name() string { return "year-display-width" }

func (yearDisplayWidthNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if strings.EqualFold(c.Type, "year") {
			c.Length = nil
		}
	}
	return ct
}
