package typeconv

// MySQLTypeMapper is an identity mapper (MySQL to MySQL)
type MySQLTypeMapper struct{}

var _ Mapper = (*MySQLTypeMapper)(nil)

func (m *MySQLTypeMapper) MapType(mysqlType string) string {
	return mysqlType
}

func (m *MySQLTypeMapper) MapValue(value interface{}, mysqlType string) (interface{}, error) {
	return value, nil
}
