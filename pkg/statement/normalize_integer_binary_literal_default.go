package statement

import (
	"encoding/binary"
	"strconv"
)

func init() { registerNormalizer(integerBinaryLiteralDefaultNormalizer{}) }

// integerBinaryLiteralDefaultNormalizer rewrites a hex or bit literal DEFAULT
// on an integer column to the decimal integer MySQL stores. MySQL reads the
// literal's bytes as a big-endian unsigned integer and SHOW CREATE TABLE
// reports the result as a number, so `b int DEFAULT 0x1A` comes back as
// `b int DEFAULT '26'`. Without the rule the literal diffs against the live
// column and emits a MODIFY COLUMN that MySQL stores as the number again, so
// the next diff emits it again. Verified against MySQL 8.0.43:
//
//	int DEFAULT 0x1A                              -> '26'
//	int DEFAULT b'1010'                           -> '10'
//	int DEFAULT b'0'                              -> '0'
//	int unsigned DEFAULT 0xFFFFFFFF               -> '4294967295'
//	bigint unsigned DEFAULT 0xFFFFFFFFFFFFFFFF    -> '18446744073709551615'
//	int DEFAULT 0xFFFFFFFF                        -> error 1067 (out of range)
//	int DEFAULT x'' / b''                         -> error 1067
//	bigint unsigned DEFAULT 0x00FFFFFFFFFFFFFFFF  -> error 1067 (9 bytes, even with a leading zero)
//	int DEFAULT (0x1A)                            -> (0x1a)  (an expression is stored as written)
//
// The value is recorded as a [DefaultKindNumber], which is emitted bare.
// Quotedness is not part of column identity on a numeric column (see
// [columnsEqual]), so it compares equal to the live '26'.
//
// A value out of range for the column's type is converted anyway. MySQL
// rejects it in either spelling, so the MODIFY fails the same way it would
// have with the literal. An empty literal and one longer than 8 bytes are left
// alone, because they have no integer value (see [binaryLiteralValue]).
//
// Only the integer types. decimal pads to its scale (decimal(5,2) DEFAULT 0x1A
// stores '26.00') and year puts the value through its own interpretation (year
// DEFAULT 0x07 stores '2007'), so neither stores the plain integer.
type integerBinaryLiteralDefaultNormalizer struct{}

func (integerBinaryLiteralDefaultNormalizer) Name() string {
	return "integer-binary-literal-default"
}

func (integerBinaryLiteralDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || !isIntegerColumnType(c.Type) {
			continue
		}
		value, ok := binaryLiteralValue(c)
		if !ok {
			continue
		}
		stored := strconv.FormatUint(value, 10)
		c.Default, c.DefaultKind = &stored, DefaultKindNumber
	}
	return ct
}

// binaryLiteralValue returns a column's hex or bit literal DEFAULT as the
// big-endian unsigned integer MySQL reads it as in a numeric context, or false
// for any other default. MySQL rejects an empty literal and one longer than 8
// bytes as a default on an integer or bit column, even when the extra bytes are
// leading zeros, so those are false too.
func binaryLiteralValue(c *Column) (uint64, bool) {
	b, ok := binaryLiteralBytes(c)
	if !ok || len(b) == 0 {
		return 0, false
	}
	return bigEndianUint64(b)
}

// bigEndianUint64 returns up to 8 bytes as a big-endian unsigned integer, and
// false for more than 8. No bytes is 0.
func bigEndianUint64(b string) (uint64, bool) {
	if len(b) > 8 {
		return 0, false
	}
	var buf [8]byte
	copy(buf[8-len(b):], b)
	return binary.BigEndian.Uint64(buf[:]), true
}
