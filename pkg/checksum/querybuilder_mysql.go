package checksum

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/table"
)

// MySQLQueryBuilder builds queries for MySQL-to-MySQL checksums
type MySQLQueryBuilder struct{}

var _ queryBuilder = (*MySQLQueryBuilder)(nil)

func (b *MySQLQueryBuilder) sourceQuery(chunk *table.Chunk) string {
	cols := b.intersectColumns(chunk)
	return fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, COUNT(*) as c FROM %s WHERE %s",
		cols,
		chunk.Table.QuotedName,
		chunk.String(),
	)
}

func (b *MySQLQueryBuilder) targetQuery(chunk *table.Chunk) string {
	cols := b.intersectColumns(chunk)
	return fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, COUNT(*) as c FROM %s WHERE %s",
		cols,
		chunk.NewTable.QuotedName,
		chunk.String(),
	)
}

func (b *MySQLQueryBuilder) formatTableName(quotedName string) string {
	// MySQL uses backticks, no conversion needed
	return quotedName
}

func (b *MySQLQueryBuilder) formatWhereClause(whereClause string) string {
	// MySQL uses backticks, no conversion needed
	return whereClause
}

func (b *MySQLQueryBuilder) isCrossDatabase() bool {
	return false
}

func (b *MySQLQueryBuilder) targetType() string {
	return "mysql"
}

func (b *MySQLQueryBuilder) intersectColumns(chunk *table.Chunk) string {
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
