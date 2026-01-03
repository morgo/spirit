package checksum

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/table"
)

// PostgreSQLQueryBuilder builds queries for MySQL-to-PostgreSQL checksums
type PostgreSQLQueryBuilder struct{}

var _ queryBuilder = (*PostgreSQLQueryBuilder)(nil)

func (b *PostgreSQLQueryBuilder) sourceQuery(chunk *table.Chunk) string {
	cols := b.intersectColumnsMySQL(chunk)
	return fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, COUNT(*) as c FROM %s WHERE %s",
		cols,
		chunk.Table.QuotedName,
		chunk.String(),
	)
}

func (b *PostgreSQLQueryBuilder) targetQuery(chunk *table.Chunk) string {
	cols := b.intersectColumnsPostgreSQL(chunk)
	tableName := b.formatTableName(chunk.NewTable.QuotedName)
	whereClause := b.formatWhereClause(chunk.String())

	// Use crc32() to match MySQL's CRC32 function
	return fmt.Sprintf(
		"SELECT BIT_XOR(crc32((%s)::bytea)) as checksum, COUNT(*) as c FROM %s WHERE %s",
		cols,
		tableName,
		whereClause,
	)
}

func (b *PostgreSQLQueryBuilder) formatTableName(quotedName string) string {
	// Convert table name: remove ALL backticks and use unquoted lowercase
	tableName := strings.ReplaceAll(quotedName, "`", "")
	tableName = strings.ToLower(tableName)

	// For cross-database sync, strip the MySQL schema prefix
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		tableName = tableName[idx+1:]
	}

	return tableName
}

func (b *PostgreSQLQueryBuilder) formatWhereClause(whereClause string) string {
	// Convert WHERE clause: remove all backticks from column names
	return strings.ReplaceAll(whereClause, "`", "")
}

func (b *PostgreSQLQueryBuilder) isCrossDatabase() bool {
	return true
}

func (b *PostgreSQLQueryBuilder) targetType() string {
	return "postgresql"
}

func (b *PostgreSQLQueryBuilder) intersectColumnsMySQL(chunk *table.Chunk) string {
	var intersection []string
	for _, col := range chunk.Table.NonGeneratedColumns {
		for _, col2 := range chunk.NewTable.NonGeneratedColumns {
			if col == col2 {
				intersection = append(intersection,
					fmt.Sprintf("IFNULL(`%s`, '')", col),
					fmt.Sprintf("ISNULL(`%s`)", col),
				)
			}
		}
	}
	return strings.Join(intersection, ", ")
}

func (b *PostgreSQLQueryBuilder) intersectColumnsPostgreSQL(chunk *table.Chunk) string {
	var parts []string
	for _, col := range chunk.Table.NonGeneratedColumns {
		for _, col2 := range chunk.NewTable.NonGeneratedColumns {
			if col == col2 {
				// PostgreSQL syntax: COALESCE and CASE for NULL handling
				parts = append(parts,
					fmt.Sprintf("COALESCE(CAST(%s AS TEXT), '')", col),
					fmt.Sprintf("CASE WHEN %s IS NULL THEN '1' ELSE '0' END", col),
				)
			}
		}
	}
	// Concatenate with || operator
	return strings.Join(parts, " || ")
}
