package lint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&AllowCharset{
		charsets: []string{"utf8mb4"},
	})
}

type AllowCharset struct {
	charsets []string
}

func (l *AllowCharset) Name() string {
	return "allow_charset"
}

func (l *AllowCharset) Description() string {
	return "Checks that only allowed character sets are used"
}

func (l *AllowCharset) String() string {
	return Stringer(l)
}

func (l *AllowCharset) Configure(config map[string]string) error {
	for k, v := range config {
		switch k {
		case "charsets":
			l.charsets = strings.Split(v, ",")
		default:
			return fmt.Errorf("unknown configuration key for %q: %s", l.Name(), k)
		}
	}
	return nil
}

func (l *AllowCharset) DefaultConfig() map[string]string {
	return map[string]string{
		"charsets": "utf8mb4",
	}
}

func (l *AllowCharset) Lint(createTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// TODO: do we care about supporting character sets that are valid for MySQL but not valid for the TiDB parser? For example big5
	suggestion := "Use a supported character set: " + strings.Join(l.charsets, ", ")
	for ct := range CreateTableStatements(createTables, changes) {
		if ct.TableOptions != nil && ct.TableOptions.Charset != nil && !slices.Contains(l.charsets, *ct.TableOptions.Charset) {
			violations = append(violations, Violation{
				Linter:     l,
				Location:   &Location{Table: ct.TableName},
				Message:    fmt.Sprintf("Character set %q given for table %q is not allowed", *ct.TableOptions.Charset, ct.TableName),
				Suggestion: &suggestion,
			})
		}
		for _, column := range ct.Columns {
			charset := column.Raw.Tp.GetCharset()
			if charset != "" && !slices.Contains(l.charsets, charset) {
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:  ct.TableName,
						Column: &column.Name,
					},
					Message:    fmt.Sprintf("Column %q has unsupported character set %q", column.Name, charset),
					Severity:   SeverityError,
					Suggestion: &suggestion,
				})
			}
		}
	}
	return violations
}
