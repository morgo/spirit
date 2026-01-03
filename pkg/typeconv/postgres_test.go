package typeconv

import (
	"fmt"
	"testing"
)

func TestPostgreSQLTypeMapper_MapType(t *testing.T) {
	mapper := &PostgreSQLTypeMapper{}

	tests := []struct {
		name      string
		mysqlType string
		want      string
	}{
		// Integer types
		{"tinyint(1) boolean", "TINYINT(1)", "BOOLEAN"},
		{"tinyint", "TINYINT", "SMALLINT"},
		{"smallint", "SMALLINT", "SMALLINT"},
		{"mediumint", "MEDIUMINT", "INTEGER"},
		{"int", "INT", "INTEGER"},
		{"integer", "INTEGER", "INTEGER"},
		{"bigint", "BIGINT", "BIGINT"},
		{"int unsigned", "INT UNSIGNED", "INTEGER"},

		// Floating point
		{"float", "FLOAT", "REAL"},
		{"double", "DOUBLE", "DOUBLE PRECISION"},

		// Decimal
		{"decimal", "DECIMAL", "NUMERIC"},
		{"decimal(10)", "DECIMAL(10)", "NUMERIC(10)"},
		{"decimal(10,2)", "DECIMAL(10,2)", "NUMERIC(10,2)"},
		{"numeric(15,4)", "NUMERIC(15,4)", "NUMERIC(15,4)"},

		// Character types
		{"char", "CHAR(10)", "CHAR(10)"},
		{"varchar", "VARCHAR(255)", "VARCHAR(255)"},
		{"tinytext", "TINYTEXT", "TEXT"},
		{"text", "TEXT", "TEXT"},
		{"mediumtext", "MEDIUMTEXT", "TEXT"},
		{"longtext", "LONGTEXT", "TEXT"},

		// Binary types
		{"binary", "BINARY(16)", "BYTEA"},
		{"varbinary", "VARBINARY(255)", "BYTEA"},
		{"blob", "BLOB", "BYTEA"},
		{"tinyblob", "TINYBLOB", "BYTEA"},
		{"mediumblob", "MEDIUMBLOB", "BYTEA"},
		{"longblob", "LONGBLOB", "BYTEA"},

		// Date and time
		{"date", "DATE", "DATE"},
		{"time", "TIME", "TIME"},
		{"datetime", "DATETIME", "TIMESTAMP"},
		{"timestamp", "TIMESTAMP", "TIMESTAMP"},
		{"year", "YEAR", "SMALLINT"},

		// JSON
		{"json", "JSON", "JSONB"},

		// ENUM and SET
		{"enum", "ENUM('a','b','c')", "TEXT"},
		{"set", "SET('a','b','c')", "TEXT[]"},

		// Bit
		{"bit", "BIT(1)", "BIT(1)"},
		{"bit(8)", "BIT(8)", "BIT(8)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapper.MapType(tt.mysqlType)
			if got != tt.want {
				t.Errorf("MapType(%q) = %q, want %q", tt.mysqlType, got, tt.want)
			}
		})
	}
}

func TestPostgreSQLTypeMapper_MapValue(t *testing.T) {
	mapper := &PostgreSQLTypeMapper{}

	tests := []struct {
		name      string
		value     interface{}
		mysqlType string
		want      interface{}
		wantErr   bool
	}{
		{"nil value", nil, "INT", nil, false},
		{"int value", int64(42), "INT", int64(42), false},
		{"tinyint(1) true", int64(1), "TINYINT(1)", true, false},
		{"tinyint(1) false", int64(0), "TINYINT(1)", false, false},
		{"tinyint(1) bool true", true, "TINYINT(1)", true, false},
		{"tinyint(1) bool false", false, "TINYINT(1)", false, false},
		{"tinyint regular", int64(100), "TINYINT", int64(100), false},
		{"enum value", "active", "ENUM('active','inactive')", "active", false},
		{"set value", "read,write", "SET('read','write','execute')", "{read,write}", false},
		{"set empty", "", "SET('a','b')", "{}", false},
		{"json value", `{"key":"value"}`, "JSON", `{"key":"value"}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mapper.MapValue(tt.value, tt.mysqlType)
			if (err != nil) != tt.wantErr {
				t.Errorf("MapValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MapValue(%v, %q) = %v, want %v", tt.value, tt.mysqlType, got, tt.want)
			}
		})
	}
}

func TestGetTypeMapper(t *testing.T) {
	tests := []struct {
		name       string
		targetType TargetType
		wantType   string
	}{
		{"mysql", TargetTypeMySQL, "*typeconv.MySQLTypeMapper"},
		{"postgresql", TargetTypePostgreSQL, "*typeconv.PostgreSQLTypeMapper"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := GetTypeMapper(tt.targetType)
			typeName := fmt.Sprintf("%T", mapper)
			if typeName != tt.wantType {
				t.Errorf("GetTypeMapper(%v) = %s, want %s", tt.targetType, typeName, tt.wantType)
			}
		})
	}
}

func TestExtractTypeInfo(t *testing.T) {
	tests := []struct {
		name      string
		mysqlType string
		want      TypeInfo
	}{
		{
			name:      "simple int",
			mysqlType: "INT",
			want:      TypeInfo{BaseType: "INT", Length: 0, Precision: 0, Scale: 0, Unsigned: false},
		},
		{
			name:      "int unsigned",
			mysqlType: "INT UNSIGNED",
			want:      TypeInfo{BaseType: "INT", Length: 0, Precision: 0, Scale: 0, Unsigned: true},
		},
		{
			name:      "varchar with length",
			mysqlType: "VARCHAR(255)",
			want:      TypeInfo{BaseType: "VARCHAR", Length: 255, Precision: 0, Scale: 0, Unsigned: false},
		},
		{
			name:      "decimal with precision",
			mysqlType: "DECIMAL(10,2)",
			want:      TypeInfo{BaseType: "DECIMAL", Length: 0, Precision: 10, Scale: 2, Unsigned: false},
		},
		{
			name:      "decimal unsigned",
			mysqlType: "DECIMAL(10,2) UNSIGNED",
			want:      TypeInfo{BaseType: "DECIMAL", Length: 0, Precision: 10, Scale: 2, Unsigned: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractTypeInfo(tt.mysqlType)
			if got != tt.want {
				t.Errorf("ExtractTypeInfo(%q) = %+v, want %+v", tt.mysqlType, got, tt.want)
			}
		})
	}
}
