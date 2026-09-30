package statement

import (
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
// Left alone: expression defaults, NULL, a negative value (MySQL rejects it on
// an unsigned column, except '-0' forms that round to zero, which this rule
// does not convert), a string that is not a number, and ZEROFILL on decimal,
// float and double, which pad in their own formats.
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
		// 2.5e+00) and a decimal literal without one. MySQL rounds a float to
		// even and a decimal half up.
		return roundedUnsignedText(*c.Default, strings.ContainsAny(*c.Default, "eE"))
	case DefaultKindString:
		// MySQL ignores surrounding spaces and rounds a string half up,
		// exponent or not.
		return roundedUnsignedText(strings.Trim(*c.Default, " "), false)
	case DefaultKindUnknown:
	}
	return "", false
}

// unsignedNumberPattern matches a non-negative decimal number with an optional
// fraction and exponent: 5, +5, 007, 5., .5, 2.5, 1e1, 2.5e+00.
var unsignedNumberPattern = regexp.MustCompile(`^\+?([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$`)

// roundedUnsignedText rounds a non-negative decimal number text to an integer
// and returns it in decimal digits. A tie rounds up, or to even when halfEven
// is set. It returns false for any other text, including a negative number.
func roundedUnsignedText(text string, halfEven bool) (string, bool) {
	m := unsignedNumberPattern.FindStringSubmatch(text)
	if m == nil || m[1]+m[2] == "" {
		return "", false
	}
	exponent := 0
	if m[3] != "" {
		e, err := strconv.Atoi(m[3])
		// Past 10^±64 the value is either 0 or out of any integer type's
		// range; leave it for MySQL to accept or reject as written.
		if err != nil || e > 64 || e < -64 {
			return "", false
		}
		exponent = e
	}
	digits, ok := new(big.Int).SetString(m[1]+m[2], 10)
	if !ok {
		return "", false
	}
	// The number is digits × 10^-scale.
	scale := len(m[2]) - exponent
	ten := big.NewInt(10)
	if scale <= 0 {
		return digits.Mul(digits, new(big.Int).Exp(ten, big.NewInt(int64(-scale)), nil)).String(), true
	}
	divisor := new(big.Int).Exp(ten, big.NewInt(int64(scale)), nil)
	quotient, remainder := new(big.Int).QuoRem(digits, divisor, new(big.Int))
	switch remainder.Lsh(remainder, 1).Cmp(divisor) {
	case 1:
		quotient.Add(quotient, big.NewInt(1))
	case 0:
		if !halfEven || quotient.Bit(0) == 1 {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	return quotient.String(), true
}
