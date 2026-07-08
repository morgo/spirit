package statement

import "strconv"

// This file holds nil-safe accessor helpers for TableOptions, used by the
// table-option diffing in diff.go.

// Helper methods for TableOptions to handle nil safely
func (to *TableOptions) getEngine() *string {
	if to == nil {
		return nil
	}
	return to.Engine
}

func (to *TableOptions) getCharset() *string {
	if to == nil {
		return nil
	}
	return to.Charset
}

func (to *TableOptions) getCollation() *string {
	if to == nil {
		return nil
	}
	return to.Collation
}

func (to *TableOptions) getComment() *string {
	if to == nil {
		return nil
	}
	return to.Comment
}

func (to *TableOptions) getRowFormat() *string {
	if to == nil {
		return nil
	}
	return to.RowFormat
}

func (to *TableOptions) getAutoIncrement() *string {
	if to == nil || to.AutoIncrement == nil {
		return nil
	}
	s := strconv.FormatUint(*to.AutoIncrement, 10)
	return &s
}
