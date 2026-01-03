package typeconv

// Mapper maps MySQL types to target database types
type Mapper interface {
	MapType(mysqlType string) string
	MapValue(value interface{}, mysqlType string) (interface{}, error)
}

// TargetType represents the type of target database
type TargetType string

const (
	TargetTypeMySQL      TargetType = "mysql"
	TargetTypePostgreSQL TargetType = "postgresql"
)

// GetTypeMapper returns the appropriate type mapper for the target type
func GetTypeMapper(targetType TargetType) Mapper {
	switch targetType {
	case TargetTypeMySQL:
		return &MySQLTypeMapper{}
	case TargetTypePostgreSQL:
		return &PostgreSQLTypeMapper{}
	default:
		// Default to MySQL (identity mapping)
		return &MySQLTypeMapper{}
	}
}
