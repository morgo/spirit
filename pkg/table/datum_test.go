package table

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatum(t *testing.T) {
	signed := NewDatum(1, signedType)
	unsigned := NewDatum(uint(1), unsignedType)

	assert.Equal(t, "1", signed.String())
	assert.Equal(t, "1", unsigned.String())

	assert.Equal(t, strconv.Itoa(math.MinInt64), signed.MinValue().String())
	assert.Equal(t, strconv.Itoa(math.MaxInt64), signed.MaxValue().String())
	assert.Equal(t, "0", unsigned.MinValue().String())
	assert.Equal(t, "18446744073709551615", unsigned.MaxValue().String())

	newsigned := signed.Add(10)
	newunsigned := unsigned.Add(10)
	assert.Equal(t, "11", newsigned.String())
	assert.Equal(t, "11", newunsigned.String())

	assert.True(t, newsigned.GreaterThanOrEqual(signed))
	assert.True(t, newunsigned.GreaterThanOrEqual(unsigned))

	// Test that add operations do not overflow. i.e.
	// We initialize the values to max-10 of the range, but then add 100 to each.
	// The add operation truncates: so both should equal the maxValue exactly.
	overflowSigned := NewDatum(uint64(math.MaxInt64)-10, signedType) // wrong type, converts.
	overflowUnsigned := NewDatum(uint64(math.MaxUint64)-10, unsignedType)
	assert.Equal(t, strconv.Itoa(math.MaxInt64), overflowSigned.Add(100).String())
	assert.Equal(t, "18446744073709551615", overflowUnsigned.Add(100).String())

	// Test unsigned with signed input
	unsigned = NewDatum(int(1), unsignedType)
	assert.Equal(t, "1", unsigned.String())

	// Test binary type.
	binary := NewDatum("0", binaryType)
	assert.Equal(t, `"0"`, binary.String())
}

func TestDatumInt32ToUnsigned(t *testing.T) {
	// Test that int32 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned INT columns as signed int32

	// Positive int32 value
	positiveInt32 := int32(123456)
	d1 := NewDatum(positiveInt32, unsignedType)
	assert.Equal(t, uint64(123456), d1.Val)
	assert.Equal(t, "123456", d1.String())

	// Negative int32 value (large unsigned value)
	// -840443956 as int32 = 3454523340 as uint32
	negativeInt32 := int32(-840443956)
	d2 := NewDatum(negativeInt32, unsignedType)
	expectedUint32 := uint32(negativeInt32) // Reinterpret bits as unsigned
	assert.Equal(t, uint64(expectedUint32), d2.Val)
	assert.Equal(t, uint64(3454523340), d2.Val)
	assert.Equal(t, "3454523340", d2.String())

	// Edge case: int32 max value
	maxInt32 := int32(math.MaxInt32)
	d3 := NewDatum(maxInt32, unsignedType)
	assert.Equal(t, uint64(math.MaxInt32), d3.Val)
	assert.Equal(t, "2147483647", d3.String())

	// Edge case: int32 min value (becomes max uint32 range)
	minInt32 := int32(math.MinInt32)
	d4 := NewDatum(minInt32, unsignedType)
	expectedUint32Min := uint32(minInt32)
	assert.Equal(t, uint64(expectedUint32Min), d4.Val)
	assert.Equal(t, uint64(2147483648), d4.Val)
	assert.Equal(t, "2147483648", d4.String())

	// Test uint32 values pass through correctly
	positiveUint32 := uint32(3454523340)
	d5 := NewDatum(positiveUint32, unsignedType)
	assert.Equal(t, uint64(3454523340), d5.Val)
	assert.Equal(t, "3454523340", d5.String())
}

// TestNewDatumFromValue tests that NewDatumFromValue properly handles various MySQL types
// This ensures compatibility with the old EscapeMySQLType function
func TestNewDatumFromValue(t *testing.T) {
	// Test NULL values
	assert.Equal(t, "NULL", NewDatumFromValue(nil, "INT").String())
	assert.Equal(t, "NULL", NewDatumFromValue(nil, "VARCHAR(255)").String())
	assert.Equal(t, "NULL", NewDatumFromValue(nil, "JSON").String())

	// Test integer types
	intBytes := []byte("123")
	assert.Equal(t, "123", NewDatumFromValue(intBytes, "INT").String())
	assert.Equal(t, "123", NewDatumFromValue(intBytes, "BIGINT").String())
	assert.Equal(t, "456", NewDatumFromValue(456, "INT").String())
	assert.Equal(t, "123", NewDatumFromValue(intBytes, "INT(11)").String())

	// Test VARCHAR/TEXT types
	textBytes := []byte("hello world")
	assert.Equal(t, "\"hello world\"", NewDatumFromValue(textBytes, "VARCHAR(255)").String())
	assert.Equal(t, "\"hello world\"", NewDatumFromValue("hello world", "TEXT").String())
	assert.Equal(t, "\"hello world\"", NewDatumFromValue(textBytes, "CHAR(50)").String())

	// Test with quotes that need escaping
	textWithQuotes := []byte("hello 'world'")
	assert.Equal(t, "\"hello \\'world\\'\"", NewDatumFromValue(textWithQuotes, "VARCHAR(255)").String())

	// Test DATETIME/TIMESTAMP types
	timeBytes := []byte("2023-01-01 12:00:00")
	assert.Equal(t, "\"2023-01-01 12:00:00\"", NewDatumFromValue(timeBytes, "DATETIME").String())
	assert.Equal(t, "\"2023-01-01 12:00:00\"", NewDatumFromValue(timeBytes, "TIMESTAMP").String())

	// Test float types
	floatBytes := []byte("123.45")
	assert.Equal(t, "\"123.45\"", NewDatumFromValue(floatBytes, "FLOAT").String())
	assert.Equal(t, "\"123.45\"", NewDatumFromValue(floatBytes, "DOUBLE").String())

	// Test VARBINARY/BLOB types - should use hex encoding
	binaryData := []byte{0x01, 0x02, 0x03}
	result := NewDatumFromValue(binaryData, "VARBINARY(255)").String()
	assert.Equal(t, "0x010203", result)

	// Test BLOB types
	blobData := []byte{0xFF, 0xFE, 0xFD}
	result = NewDatumFromValue(blobData, "BLOB").String()
	assert.Equal(t, "0xfffefd", result)

	// Test JSON types - should be quoted like text
	jsonBytes := []byte(`[1, 2, 3]`)
	assert.Equal(t, "CONVERT('[1, 2, 3]' USING utf8mb4)", NewDatumFromValue(jsonBytes, "JSON").String())
	assert.Equal(t, "CONVERT('test' USING utf8mb4)", NewDatumFromValue("test", "json").String())

	// Test case insensitivity
	assert.Equal(t, "123", NewDatumFromValue(intBytes, "int").String())
	assert.Equal(t, "123", NewDatumFromValue(intBytes, "Int").String())
	assert.Equal(t, "\"hello\"", NewDatumFromValue([]byte("hello"), "varchar(100)").String())

	// Test unknown/default types
	unknownBytes := []byte("unknown data")
	assert.Equal(t, "\"unknown data\"", NewDatumFromValue(unknownBytes, "UNKNOWN_TYPE").String())
	assert.Equal(t, "\"test\"", NewDatumFromValue("test", "CUSTOM_TYPE").String())
}

// TestNewDatumFromValueBinaryString tests that binary strings are hex-encoded
func TestNewDatumFromValueBinaryString(t *testing.T) {
	// Test binary data that's not valid UTF-8
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF}
	result := NewDatumFromValue(binaryData, "VARBINARY(255)").String()
	assert.Equal(t, "0x000102ff", result)

	// Test string that starts with 0x (should be hex encoded to avoid JSON corruption)
	jsonLikeString := "0x123"
	result = NewDatumFromValue(jsonLikeString, "VARCHAR(255)").String()
	// This should be hex encoded because it starts with 0x
	assert.Equal(t, "0x3078313233", result) // hex of "0x123"

	// Test normal UTF-8 string
	normalString := "hello"
	result = NewDatumFromValue(normalString, "VARCHAR(255)").String()
	assert.Equal(t, "\"hello\"", result)
}
