package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// This file holds the helpers that render parsed schema elements back into the
// SQL fragments used to build ALTER TABLE clauses. They are free functions; the
// CreateTable receivers that assemble the statements live in diff.go.

// formatColumnDefinition formats a column definition for ALTER TABLE
func formatColumnDefinition(col *Column) string {
	var parts []string

	// Column name and type
	typeDef := col.Type

	// Determine the full type definition including length/precision/values
	switch {
	case col.Type == "enum" && len(col.EnumValues) > 0:
		var values []string
		for _, v := range col.EnumValues {
			values = append(values, fmt.Sprintf("'%s'", sqlescape.EscapeString(v)))
		}
		typeDef = fmt.Sprintf("enum(%s)", strings.Join(values, ","))
	case col.Type == "set" && len(col.SetValues) > 0:
		var values []string
		for _, v := range col.SetValues {
			values = append(values, fmt.Sprintf("'%s'", sqlescape.EscapeString(v)))
		}
		typeDef = fmt.Sprintf("set(%s)", strings.Join(values, ","))
	case col.Precision != nil && col.Scale != nil:
		// DECIMAL(precision, scale) - check Precision/Scale BEFORE Length!
		// DECIMAL columns have both Length and Precision/Scale set, but we want Precision/Scale
		typeDef = fmt.Sprintf("%s(%d,%d)", col.Type, *col.Precision, *col.Scale)
	case col.Precision != nil:
		// DECIMAL(precision) or other numeric types with precision only
		typeDef = fmt.Sprintf("%s(%d)", col.Type, *col.Precision)
	case col.Length != nil:
		// VARCHAR, CHAR, etc.
		typeDef = fmt.Sprintf("%s(%d)", col.Type, *col.Length)
	}

	if col.Unsigned != nil && *col.Unsigned {
		typeDef += " unsigned"
	}

	if col.Zerofill != nil && *col.Zerofill {
		typeDef += " zerofill"
	}

	parts = append(parts, fmt.Sprintf("%s %s", sqlescape.EscapeIdentifier(col.Name), typeDef))

	// Charset and collation (skip for binary types and JSON)
	isBinaryType := col.Type == "varbinary" || col.Type == "binary" ||
		col.Type == "tinyblob" || col.Type == "blob" ||
		col.Type == "mediumblob" || col.Type == "longblob"
	isJSONType := col.Type == "json"

	if !isBinaryType && !isJSONType {
		if col.Charset != nil {
			parts = append(parts, fmt.Sprintf("CHARACTER SET %s", *col.Charset))
		}
		if col.Collation != nil {
			parts = append(parts, fmt.Sprintf("COLLATE %s", *col.Collation))
		}
	}

	// Generated column clause — comes directly after the data type (and any
	// charset/collation), before the NULL/NOT NULL attribute:
	// col_name type GENERATED ALWAYS AS (expr) STORED|VIRTUAL [NOT NULL|NULL] ...
	if col.GeneratedExpr != nil {
		genClause := fmt.Sprintf("GENERATED ALWAYS AS (%s)", *col.GeneratedExpr)
		if col.GeneratedStored {
			genClause += " STORED"
		} else {
			genClause += " VIRTUAL"
		}
		parts = append(parts, genClause)
	}

	// Nullable
	if !col.Nullable {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}

	// SRID attribute for spatial columns. MySQL's SHOW CREATE TABLE places
	// this after the NULL/NOT NULL attribute (as /*!80003 SRID n */).
	if col.SRID != nil {
		parts = append(parts, fmt.Sprintf("SRID %d", *col.SRID))
	}

	// Default value (not permitted on generated columns)
	if col.Default != nil && col.GeneratedExpr == nil {
		defaultVal := *col.Default
		switch {
		case col.DefaultIsExpr:
			// Expression defaults must be wrapped in parentheses, e.g. DEFAULT (json_object())
			parts = append(parts, fmt.Sprintf("DEFAULT (%s)", defaultVal))
		case col.DefaultIsString:
			// Quoted string literal. The stored value is the true raw value
			// (unescaped at parse time), so quote+escape exactly once. This
			// must bypass the needsQuotes heuristic: a literal 'TRUE' or
			// 'NULL' or '2020' has to stay quoted, otherwise MySQL would
			// store the keyword/number instead of the string.
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", sqlescape.EscapeString(defaultVal)))
		case needsQuotes(defaultVal):
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", sqlescape.EscapeString(defaultVal)))
		default:
			parts = append(parts, fmt.Sprintf("DEFAULT %s", defaultVal))
		}
	}

	// ON UPDATE (TIMESTAMP/DATETIME auto-update) — comes after DEFAULT
	if col.OnUpdate != nil {
		parts = append(parts, fmt.Sprintf("ON UPDATE %s", *col.OnUpdate))
	}

	// Auto increment
	if col.AutoInc {
		parts = append(parts, "AUTO_INCREMENT")
	}

	// Comment
	if col.Comment != nil {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", sqlescape.EscapeString(*col.Comment)))
	}

	// NOTE: column-level CHECK constraints are deliberately not emitted here.
	// The parser hoists them into table-level constraints (see
	// columnCheckNormalizer), so they are emitted as ADD CONSTRAINT ... CHECK
	// by diffConstraints. Emitting them inline in a MODIFY/ADD COLUMN would
	// make MySQL hoist them to a table-level CHECK with an auto-name, breaking
	// re-diff convergence and risking a spurious DROP CHECK.

	return strings.Join(parts, " ")
}

// formatAddIndex formats an ADD INDEX clause
func formatAddIndex(idx *Index) string {
	var parts []string

	// Build the ADD clause: keyword + optional name.
	// The name is omitted when empty (safety net; indexNameNormalizer should
	// have already assigned one during parsing).
	var keyword string
	switch idx.Type {
	case "PRIMARY KEY":
		keyword = "ADD PRIMARY KEY"
	case "UNIQUE":
		keyword = "ADD UNIQUE INDEX"
	case "FULLTEXT":
		keyword = "ADD FULLTEXT INDEX"
	case "SPATIAL":
		keyword = "ADD SPATIAL INDEX"
	default:
		keyword = "ADD INDEX"
	}
	if idx.Type != "PRIMARY KEY" && idx.Name != "" {
		keyword += " " + sqlescape.EscapeIdentifier(idx.Name)
	}
	parts = append(parts, keyword)

	// Columns - use ColumnList if available for full details (prefix, expressions)
	var columns []string
	if len(idx.ColumnList) > 0 {
		for _, col := range idx.ColumnList {
			var colStr string
			if col.Expression != nil {
				// Expression index (functional index) - needs double parentheses
				colStr = fmt.Sprintf("(%s)", *col.Expression)
			} else {
				// Regular column reference
				colStr = sqlescape.EscapeIdentifier(col.Name)
				if col.Length != nil {
					colStr += fmt.Sprintf("(%d)", *col.Length)
				}
			}
			// Descending key part (MySQL 8.0+). ASC is MySQL's canonical
			// default and is never emitted explicitly.
			if col.Desc {
				colStr += " DESC"
			}
			columns = append(columns, colStr)
		}
	} else {
		// Fall back to simple column names
		for _, col := range idx.Columns {
			columns = append(columns, sqlescape.EscapeIdentifier(col))
		}
	}
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(columns, ", ")))

	// USING clause
	if idx.Using != nil {
		parts = append(parts, fmt.Sprintf("USING %s", *idx.Using))
	}

	// KEY_BLOCK_SIZE
	if idx.KeyBlockSize != nil {
		parts = append(parts, fmt.Sprintf("KEY_BLOCK_SIZE=%d", *idx.KeyBlockSize))
	}

	// WITH PARSER (FULLTEXT indexes)
	if idx.ParserName != nil {
		parts = append(parts, fmt.Sprintf("WITH PARSER %s", *idx.ParserName))
	}

	// Comment
	if idx.Comment != nil {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", sqlescape.EscapeString(*idx.Comment)))
	}

	// Visibility
	if idx.Invisible != nil && *idx.Invisible {
		parts = append(parts, "INVISIBLE")
	}

	return strings.Join(parts, " ")
}

// formatAddConstraint formats an ADD CONSTRAINT clause
func formatAddConstraint(constr *Constraint) string {
	var parts []string

	switch constr.Type {
	case "CHECK":
		clause := ""
		if constr.Name != "" {
			clause = fmt.Sprintf("ADD CONSTRAINT %s CHECK (%s)", sqlescape.EscapeIdentifier(constr.Name), *constr.Expression)
		} else {
			clause = fmt.Sprintf("ADD CHECK (%s)", *constr.Expression)
		}
		// ENFORCED is MySQL's default and is omitted (as SHOW CREATE TABLE
		// does), so the ADD round-trips; NOT ENFORCED must be spelled out or
		// the re-added constraint would silently become enforced.
		if constr.NotEnforced {
			clause += " NOT ENFORCED"
		}
		parts = append(parts, clause)
	case "FOREIGN KEY":
		var columns []string
		for _, col := range constr.Columns {
			columns = append(columns, sqlescape.EscapeIdentifier(col))
		}
		var refColumns []string
		for _, col := range constr.References.Columns {
			refColumns = append(refColumns, sqlescape.EscapeIdentifier(col))
		}

		var fkClause string
		if constr.Name != "" {
			fkClause = fmt.Sprintf("ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				sqlescape.EscapeIdentifier(constr.Name),
				strings.Join(columns, ", "),
				sqlescape.EscapeIdentifier(constr.References.Table),
				strings.Join(refColumns, ", "))
		} else {
			fkClause = fmt.Sprintf("ADD FOREIGN KEY (%s) REFERENCES %s (%s)",
				strings.Join(columns, ", "),
				sqlescape.EscapeIdentifier(constr.References.Table),
				strings.Join(refColumns, ", "))
		}

		// Add ON DELETE clause if present
		if constr.References.OnDelete != nil {
			fkClause += fmt.Sprintf(" ON DELETE %s", *constr.References.OnDelete)
		}

		// Add ON UPDATE clause if present
		if constr.References.OnUpdate != nil {
			fkClause += fmt.Sprintf(" ON UPDATE %s", *constr.References.OnUpdate)
		}

		parts = append(parts, fkClause)
	}

	return strings.Join(parts, " ")
}

// formatPartitionOptions formats partition options for ALTER TABLE
func formatPartitionOptions(partOpts *PartitionOptions) string {
	var parts []string

	// Start with PARTITION BY
	parts = append(parts, "PARTITION BY")

	// Add LINEAR keyword if applicable
	if partOpts.Linear {
		parts = append(parts, "LINEAR")
	}

	// Add partition type
	parts = append(parts, partOpts.Type)

	// Add expression or columns based on partition type
	switch partOpts.Type {
	case "HASH":
		if partOpts.Expression != nil {
			parts = append(parts, fmt.Sprintf("(%s)", *partOpts.Expression))
		} else if len(partOpts.Columns) > 0 {
			// HASH can also use column names directly
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		}
	case "KEY":
		if len(partOpts.Columns) > 0 {
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		} else {
			// KEY() with empty columns uses primary key
			parts = append(parts, "()")
		}
	case "RANGE":
		if partOpts.Expression != nil {
			parts = append(parts, fmt.Sprintf("(%s)", *partOpts.Expression))
		} else if len(partOpts.Columns) > 0 {
			// RANGE COLUMNS
			parts[len(parts)-1] = "RANGE COLUMNS"
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		}
	case "LIST":
		if len(partOpts.Columns) > 0 {
			// LIST COLUMNS
			parts[len(parts)-1] = "LIST COLUMNS"
			parts = append(parts, fmt.Sprintf("(%s)", quoteIdentList(partOpts.Columns, ", ")))
		}
	}

	// Add number of partitions if specified and no explicit definitions
	if partOpts.Partitions > 0 && len(partOpts.Definitions) == 0 {
		parts = append(parts, fmt.Sprintf("PARTITIONS %d", partOpts.Partitions))
	}

	// Add partition definitions if present
	if len(partOpts.Definitions) > 0 {
		var defParts []string
		for _, def := range partOpts.Definitions {
			defParts = append(defParts, formatPartitionDefinition(&def))
		}
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(defParts, ", ")))
	}

	return strings.Join(parts, " ")
}

// formatPartitionDefinition formats a single partition definition
func formatPartitionDefinition(def *PartitionDefinition) string {
	var parts []string

	// Partition name
	parts = append(parts, "PARTITION "+sqlescape.EscapeIdentifier(def.Name))

	// Values clause
	if def.Values != nil {
		switch def.Values.Type {
		case "LESS_THAN":
			values := make([]string, len(def.Values.Values))
			for i, v := range def.Values.Values {
				values[i] = formatPartitionValue(v)
			}
			parts = append(parts, fmt.Sprintf("VALUES LESS THAN (%s)", strings.Join(values, ", ")))
		case "IN":
			values := make([]string, len(def.Values.Values))
			for i, v := range def.Values.Values {
				values[i] = formatPartitionValue(v)
			}
			parts = append(parts, fmt.Sprintf("VALUES IN (%s)", strings.Join(values, ", ")))
		case "MAXVALUE":
			parts = append(parts, "VALUES LESS THAN MAXVALUE")
		}
	}

	return strings.Join(parts, " ")
}
