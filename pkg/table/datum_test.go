package table

import (
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"

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

func TestDatumInt64ToUnsigned(t *testing.T) {
	// Test that int64 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned BIGINT columns as signed int64

	// Positive int64 value
	positiveInt64 := int64(123456789012345)
	d1 := NewDatum(positiveInt64, unsignedType)
	assert.Equal(t, uint64(123456789012345), d1.Val)
	assert.Equal(t, "123456789012345", d1.String())

	// Negative int64 value (large unsigned value)
	// -1 as int64 = max uint64
	negativeInt64 := int64(-1)
	d2 := NewDatum(negativeInt64, unsignedType)
	assert.Equal(t, uint64(negativeInt64), d2.Val)
	assert.Equal(t, uint64(math.MaxUint64), d2.Val)

	// Edge case: int64 max value
	maxInt64 := int64(math.MaxInt64)
	d3 := NewDatum(maxInt64, unsignedType)
	assert.Equal(t, uint64(math.MaxInt64), d3.Val)
}

func TestKeyBelowLowWatermarkWithNegativeInt32(t *testing.T) {
	ti := &TableInfo{
		SchemaName:        "test",
		TableName:         "t1",
		QuotedName:        "`test`.`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int unsigned"},
		KeyIsAutoInc:      true,
		minValue:          NewDatum(uint64(0), unsignedType),
		maxValue:          NewDatum(uint64(1000), unsignedType),
	}

	// Create the optimistic chunker directly
	chk := &chunkerOptimistic{
		Ti:            ti,
		ChunkerTarget: 100 * time.Millisecond,
		logger:        slog.Default(),
		watermark: &Chunk{
			UpperBound: &Boundary{Value: []Datum{NewDatum(uint64(100), unsignedType)}},
			LowerBound: &Boundary{Value: []Datum{NewDatum(uint64(0), unsignedType)}},
		},
		// We need chunkPtr to have a type for NewDatum call
		chunkPtr: NewDatum(uint64(0), unsignedType),
	}

	// KeyBelowLowWatermark receives the original typed value
	// Value arrives as int32(-12345) (representing a large unsigned int)
	// 4294954951 as uint32 is -12345 as int32
	originalVal := int32(-12345)

	// KeyBelowLowWatermark should correctly handle int32 values for unsigned columns
	// The int32 will be reinterpreted as uint32, giving us 4294954951
	// Since 4294954951 > 100 (watermark), it should return false
	assert.NotPanics(t, func() {
		result := chk.KeyBelowLowWatermark(originalVal)
		assert.False(t, result, "4294954951 should not be below watermark of 100")
	}, "KeyBelowLowWatermark should handle int32 values for unsigned columns")
}
