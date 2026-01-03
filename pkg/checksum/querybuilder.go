package checksum

import "github.com/block/spirit/pkg/table"

// queryBuilder is a strategy interface for building checksum queries
type queryBuilder interface {
	sourceQuery(chunk *table.Chunk) string
	targetQuery(chunk *table.Chunk) string
	formatTableName(quotedName string) string    // Format table name for DELETE statements
	formatWhereClause(whereClause string) string // Format WHERE clause for DELETE statements
	isCrossDatabase() bool                       // Whether this is a cross-database checksum
	targetType() string                          // Target database type for logging
}
