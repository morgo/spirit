package typeconv

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// PostgreSQLTypeMapper maps MySQL types to PostgreSQL types
type PostgreSQLTypeMapper struct{}

var _ Mapper = (*PostgreSQLTypeMapper)(nil)

var (
	// Regex to extract length/precision from type strings
	lengthRegex    = regexp.MustCompile(`\((\d+)\)`)
	precisionRegex = regexp.MustCompile(`\((\d+),(\d+)\)`)
)

func (m *PostgreSQLTypeMapper) MapType(mysqlType string) string {
	// Normalize to uppercase for comparison
	upperType := strings.ToUpper(mysqlType)

	// Extract base type (without length/precision)
	baseType := upperType
	if idx := strings.Index(upperType, "("); idx != -1 {
		baseType = upperType[:idx]
	}

	// Remove UNSIGNED keyword for comparison
	baseType = strings.TrimSpace(strings.Replace(baseType, "UNSIGNED", "", 1))

	switch baseType {
	// Integer types
	case "TINYINT":
		// Special case: TINYINT(1) is boolean in MySQL
		if strings.Contains(upperType, "TINYINT(1)") {
			return "BOOLEAN"
		}
		return "SMALLINT"
	case "SMALLINT":
		return "SMALLINT"
	case "MEDIUMINT", "INT", "INTEGER":
		return "INTEGER"
	case "BIGINT":
		return "BIGINT"

	// Floating point types
	case "FLOAT":
		return "REAL"
	case "DOUBLE", "DOUBLE PRECISION":
		return "DOUBLE PRECISION"

	// Decimal types - preserve precision and scale
	case "DECIMAL", "NUMERIC", "DEC":
		if matches := precisionRegex.FindStringSubmatch(mysqlType); len(matches) == 3 {
			return fmt.Sprintf("NUMERIC(%s,%s)", matches[1], matches[2])
		} else if matches := lengthRegex.FindStringSubmatch(mysqlType); len(matches) == 2 {
			return fmt.Sprintf("NUMERIC(%s)", matches[1])
		}
		return "NUMERIC"

	// Character types
	case "CHAR":
		if matches := lengthRegex.FindStringSubmatch(mysqlType); len(matches) == 2 {
			return fmt.Sprintf("CHAR(%s)", matches[1])
		}
		return "CHAR(1)"
	case "VARCHAR":
		if matches := lengthRegex.FindStringSubmatch(mysqlType); len(matches) == 2 {
			return fmt.Sprintf("VARCHAR(%s)", matches[1])
		}
		return "VARCHAR(255)"
	case "TINYTEXT":
		return "TEXT"
	case "TEXT":
		return "TEXT"
	case "MEDIUMTEXT":
		return "TEXT"
	case "LONGTEXT":
		return "TEXT"

	// Binary types
	case "BINARY":
		return "BYTEA"
	case "VARBINARY":
		return "BYTEA"
	case "TINYBLOB":
		return "BYTEA"
	case "BLOB":
		return "BYTEA"
	case "MEDIUMBLOB":
		return "BYTEA"
	case "LONGBLOB":
		return "BYTEA"

	// Date and time types
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "DATETIME":
		return "TIMESTAMP"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "YEAR":
		return "SMALLINT"

	// JSON type
	case "JSON":
		return "JSONB"

	// ENUM and SET types
	case "ENUM":
		// PostgreSQL doesn't have ENUM in the same way
		// We'll use TEXT with a CHECK constraint (to be added separately)
		return "TEXT"
	case "SET":
		// PostgreSQL doesn't have SET
		// We'll use TEXT[] (array of text)
		return "TEXT[]"

	// Bit types
	case "BIT":
		if matches := lengthRegex.FindStringSubmatch(mysqlType); len(matches) == 2 {
			return fmt.Sprintf("BIT(%s)", matches[1])
		}
		return "BIT(1)"

	// Spatial types (basic support)
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON":
		return "GEOMETRY"
	case "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		return "GEOMETRY"
	case "GEOMETRYCOLLECTION":
		return "GEOMETRY"

	default:
		// If we don't recognize the type, return it as-is
		// This might fail, but it's better than silently converting to something wrong
		return mysqlType
	}
}

func (m *PostgreSQLTypeMapper) MapValue(value interface{}, mysqlType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// Normalize type for comparison
	upperType := strings.ToUpper(mysqlType)
	baseType := upperType
	if idx := strings.Index(upperType, "("); idx != -1 {
		baseType = upperType[:idx]
	}
	baseType = strings.TrimSpace(strings.Replace(baseType, "UNSIGNED", "", 1))

	switch baseType {
	case "TINYINT":
		// Convert TINYINT(1) to boolean
		if strings.Contains(upperType, "TINYINT(1)") {
			switch v := value.(type) {
			case int64:
				return v != 0, nil
			case int:
				return v != 0, nil
			case bool:
				return v, nil
			case []byte:
				if len(v) > 0 {
					return v[0] != 0, nil
				}
				return false, nil
			}
		}
		return value, nil

	case "BIT":
		// PostgreSQL BIT type expects '0' or '1' strings
		switch v := value.(type) {
		case []byte:
			// MySQL returns BIT as []byte
			if len(v) > 0 {
				// Convert to binary string
				var bits strings.Builder
				for _, b := range v {
					bits.WriteString(fmt.Sprintf("%08b", b))
				}
				return bits.String(), nil
			}
			return "0", nil
		case int64:
			return fmt.Sprintf("%b", v), nil
		}
		return value, nil

	case "ENUM":
		// ENUM values are already strings, just pass through
		return value, nil

	case "SET":
		// MySQL SET is returned as a string like "value1,value2"
		// Convert to PostgreSQL array format: {"value1","value2"}
		if str, ok := value.(string); ok {
			if str == "" {
				return "{}", nil
			}
			values := strings.Split(str, ",")
			return fmt.Sprintf("{%s}", strings.Join(values, ",")), nil
		}
		return value, nil

	case "JSON":
		// JSON values should be passed as-is
		// PostgreSQL will handle the conversion to JSONB
		return value, nil

	case "YEAR":
		// YEAR is stored as SMALLINT in PostgreSQL
		// MySQL returns it as int64
		return value, nil

	default:
		// For most types, no value conversion is needed
		return value, nil
	}
}

// ExtractTypeInfo extracts base type, length, precision, and scale from a MySQL type string
type TypeInfo struct {
	BaseType  string
	Length    int
	Precision int
	Scale     int
	Unsigned  bool
}

func ExtractTypeInfo(mysqlType string) TypeInfo {
	info := TypeInfo{}

	// Normalize
	upperType := strings.ToUpper(mysqlType)

	// Check for UNSIGNED
	info.Unsigned = strings.Contains(upperType, "UNSIGNED")
	upperType = strings.TrimSpace(strings.Replace(upperType, "UNSIGNED", "", 1))

	// Extract base type
	if idx := strings.Index(upperType, "("); idx != -1 {
		info.BaseType = upperType[:idx]

		// Extract length/precision/scale
		if matches := precisionRegex.FindStringSubmatch(upperType); len(matches) == 3 {
			info.Precision, _ = strconv.Atoi(matches[1])
			info.Scale, _ = strconv.Atoi(matches[2])
		} else if matches := lengthRegex.FindStringSubmatch(upperType); len(matches) == 2 {
			info.Length, _ = strconv.Atoi(matches[1])
		}
	} else {
		info.BaseType = upperType
	}

	return info
}
