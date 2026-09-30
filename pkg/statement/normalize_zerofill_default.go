package statement

import (
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

func init() { registerNormalizer(zerofillDefaultNormalizer{}) }

// zerofillDefaultNormalizer rewrites the literal DEFAULT of a ZEROFILL integer
// column to the value MySQL stores: the integer, left-padded with zeros to the
// column's display width. SHOW CREATE TABLE reports the padded form, so a
// declared `int(10) zerofill DEFAULT 5` otherwise diffs against the live
// `DEFAULT '0000000005'`, and the emitted `MODIFY ... DEFAULT 5` is stored
// padded again, on every run. Verified against MySQL 8.0.43:
//
//	int(10) zerofill DEFAULT 5        -> DEFAULT '0000000005'
//	int(0) zerofill DEFAULT 5         -> int(10) unsigned zerofill DEFAULT '0000000005'
//	int zerofill DEFAULT 5            -> int(10) unsigned zerofill DEFAULT '0000000005'
//	tinyint(2) zerofill DEFAULT '7'   -> DEFAULT '07'
//	int(3) zerofill DEFAULT 12345     -> DEFAULT '12345'  (never truncated)
//	int(4) zerofill DEFAULT 0x10      -> DEFAULT '0016'
//	int(4) zerofill DEFAULT b'101'    -> DEFAULT '0005'
//	int(4) zerofill DEFAULT TRUE      -> DEFAULT '0001'
//	int(4) zerofill DEFAULT 2.5       -> DEFAULT '0003'  (a decimal rounds half up)
//	int(4) zerofill DEFAULT 2.5e0     -> DEFAULT '0002'  (a float rounds half to even)
//	int(4) zerofill DEFAULT '2.5e0'   -> DEFAULT '0003'  (a string rounds half up)
//	int(4) zerofill DEFAULT ' 5 '     -> DEFAULT '0005'
//	int(4) zerofill DEFAULT '\t5\n'   -> DEFAULT '0005'  (leading space/tab, trailing whitespace)
//	int(4) zerofill DEFAULT -0.0      -> DEFAULT '0000'  (a negative decimal only if exactly zero)
//	int(4) zerofill DEFAULT -0.5e0    -> DEFAULT '0000'  (a negative float that rounds to zero)
//	int(4) zerofill DEFAULT '-0.49'   -> DEFAULT '0000'  (a negative string that rounds to zero)
//	int(4) zerofill DEFAULT '5e-65'   -> DEFAULT '0000'
//	bigint(20) zerofill DEFAULT 1234567890123456789e0
//	                                  -> DEFAULT '01234567890123456768'  (the double's exact value)
//	int(4) zerofill DEFAULT (5)       -> DEFAULT (5)     (an expression is stored as written)
//	int(4) zerofill DEFAULT NULL      -> DEFAULT NULL
//
// The width is read from the parsed type (Column.Raw) rather than
// Column.Length, which other rules rewrite: a width that is unwritten or 0 is
// the type's unsigned default width (see zerofillDefaultWidths), which
// is what MySQL stores. Reading the written width keeps this rule independent
// of the order it runs in.
//
// Every literal form is converted here, rather than after
// integerBinaryLiteralDefaultNormalizer and booleanKeywordDefaultNormalizer
// have turned a hex, bit or TRUE/FALSE default into a number, so the result is
// the same whichever runs first; the value is recorded as a string, which both
// of those leave alone.
//
// Left alone: expression defaults, NULL, a negative value MySQL rejects on an
// unsigned column (-5, -0.4, '-0.5', -0.6e0), a string that is not a number,
// and ZEROFILL on decimal, float and double, which pad in their own formats.
type zerofillDefaultNormalizer struct{}

func (zerofillDefaultNormalizer) Name() string { return "zerofill-default" }

func (zerofillDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Zerofill == nil || !*c.Zerofill || !isIntegerColumnType(c.Type) {
			continue
		}
		if c.Default == nil || c.DefaultIsExpr {
			continue
		}
		width, ok := zerofillDisplayWidth(c)
		if !ok {
			continue
		}
		value, ok := zerofillDefaultValue(c)
		if !ok {
			continue
		}
		if pad := width - len(value); pad > 0 {
			value = strings.Repeat("0", pad) + value
		}
		c.Default, c.DefaultKind = &value, DefaultKindString
	}
	return ct
}

// zerofillDisplayWidth returns the display width MySQL stores a ZEROFILL
// integer column with.
func zerofillDisplayWidth(c *Column) (int, bool) {
	width := -1
	if c.Raw != nil && c.Raw.Tp != nil {
		width = c.Raw.Tp.GetFlen()
	} else if c.Length != nil {
		width = *c.Length
	}
	if width > 0 {
		return width, true
	}
	width, ok := zerofillDefaultWidths[strings.ToLower(c.Type)]
	return width, ok
}

// zerofillDefaultValue returns a literal DEFAULT as the non-negative integer
// MySQL converts it to on an integer column, in decimal digits with no
// leading zeros, or false when this rule does not convert it.
func zerofillDefaultValue(c *Column) (string, bool) {
	switch c.DefaultKind {
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		value, ok := binaryLiteralValue(c)
		if !ok {
			return "", false
		}
		return strconv.FormatUint(value, 10), true
	case DefaultKindKeywordBool:
		switch strings.ToUpper(*c.Default) {
		case "TRUE":
			return "1", true
		case "FALSE":
			return "0", true
		}
	case DefaultKindNumber:
		// The parser restores a float literal with an exponent (2.5e0 as
		// 2.5e+00) and a decimal literal without one.
		if strings.ContainsAny(*c.Default, "eE") {
			return roundedFloatText(*c.Default)
		}
		// MySQL rejects a negative decimal literal unless it is exactly zero:
		// -0.0 is accepted, -0.4 is not.
		return roundedDecimalText(*c.Default, false)
	case DefaultKindString:
		// MySQL skips leading spaces and tabs, and ignores trailing
		// whitespace; it rejects a leading newline, carriage return, form
		// feed or vertical tab. A negative string that rounds to zero ('-0.4')
		// is accepted.
		text := strings.TrimLeft(*c.Default, " \t")
		text = strings.TrimRight(text, " \t\n\r\f\v")
		return roundedDecimalText(text, true)
	case DefaultKindUnknown:
	}
	return "", false
}

// roundedFloatText rounds a float literal to an integer the way MySQL stores
// a double in an unsigned integer column: the exact value of the double,
// rounded half to even. The double is not the written decimal once it has
// more than 15-17 significant digits: 1234567890123456789e0 is stored as
// 1234567890123456768. A negative value is accepted only when it rounds to
// zero (-0.5e0 is, -0.6e0 is not). It returns false for anything else,
// including a value outside the unsigned 64-bit range.
func roundedFloatText(text string) (string, bool) {
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return "", false
	}
	f = math.RoundToEven(f)
	if f == 0 {
		return "0", true
	}
	if f < 0 || f >= 0x1p64 {
		return "", false
	}
	value, _ := big.NewFloat(f).Int(nil)
	return value.String(), true
}

// decimalNumberPattern matches a decimal number with an optional sign,
// fraction and exponent: 5, +5, -0, 007, 5., .5, 2.5, 1e1, 2.5e+00.
var decimalNumberPattern = regexp.MustCompile(`^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$`)

// roundedDecimalText rounds a decimal number text half up to an integer, as
// MySQL converts a decimal literal or a string, and returns it in decimal
// digits. A negative number is accepted only when its value is zero, or,
// with negativeRoundsToZero, when it rounds to zero. It returns false for
// any other text.
func roundedDecimalText(text string, negativeRoundsToZero bool) (string, bool) {
	m := decimalNumberPattern.FindStringSubmatch(text)
	if m == nil || m[2]+m[3] == "" {
		return "", false
	}
	digits, ok := new(big.Int).SetString(m[2]+m[3], 10)
	if !ok {
		return "", false
	}
	if digits.Sign() == 0 {
		return "0", true
	}
	exponent := 0
	if m[4] != "" {
		e, err := strconv.Atoi(m[4])
		// Past 10^64 a non-zero value is out of any integer type's range;
		// leave it for MySQL to reject as written.
		if err != nil || e > 64 {
			return "", false
		}
		exponent = e
	}
	// The number is digits × 10^-scale.
	scale := len(m[3]) - exponent
	ten := big.NewInt(10)
	var quotient *big.Int
	exact := true
	switch {
	case scale <= 0:
		quotient = digits.Mul(digits, new(big.Int).Exp(ten, big.NewInt(int64(-scale)), nil))
	case scale > len(m[2]+m[3]):
		// digits < 10^len(digits), so the value is below 0.1 and rounds to
		// zero, however small the exponent.
		quotient, exact = new(big.Int), false
	default:
		divisor := new(big.Int).Exp(ten, big.NewInt(int64(scale)), nil)
		var remainder *big.Int
		quotient, remainder = new(big.Int).QuoRem(digits, divisor, new(big.Int))
		exact = remainder.Sign() == 0
		if remainder.Lsh(remainder, 1).Cmp(divisor) >= 0 {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	if m[1] == "-" && (quotient.Sign() != 0 || !exact && !negativeRoundsToZero) {
		return "", false
	}
	return quotient.String(), true
}
