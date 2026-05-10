package lint

import (
	"fmt"
	"sort"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&TypePedanticLinter{})
}

// TypePedanticLinter enforces type consistency across tables in the same schema.
//
// Rule 1 (same_name): Columns sharing a name across tables should share a type.
// Rule 2 (inferred_fk): Columns named like {table}_id are inferred to reference
// {table}.id and should match its type. Mismatches cause implicit casts and
// index misses on JOINs.
type TypePedanticLinter struct {
	checkSameName    bool
	checkInferredFK  bool
	requireIndexed   bool
	ignoreColumns    map[string]struct{}
	fkSeverity       Severity
	sameNameSeverity Severity
}

func (l *TypePedanticLinter) Name() string { return "type_pedantic" }
func (l *TypePedanticLinter) Description() string {
	return "Cross-table column type consistency: same-name columns and inferred {table}_id foreign keys should match types"
}
func (l *TypePedanticLinter) String() string { return Stringer(l) }

func (l *TypePedanticLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "true",
		"requireIndexed":   "true",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}
}

func (l *TypePedanticLinter) Configure(config map[string]string) error {
	for k, v := range config {
		switch k {
		case "checkSameName":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.checkSameName = b
		case "checkInferredFK":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.checkInferredFK = b
		case "requireIndexed":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.requireIndexed = b
		case "ignoreColumns":
			l.ignoreColumns = parseIgnoreColumns(v)
		case "fkSeverity":
			sev, err := parseSeverityValue(v, k)
			if err != nil {
				return err
			}
			l.fkSeverity = sev
		case "sameNameSeverity":
			sev, err := parseSeverityValue(v, k)
			if err != nil {
				return err
			}
			l.sameNameSeverity = sev
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), k)
		}
	}
	return nil
}

func parseIgnoreColumns(value string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, c := range strings.Split(value, ",") {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			out[c] = struct{}{}
		}
	}
	return out
}

func parseSeverityValue(value, key string) (Severity, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "error":
		return SeverityError, nil
	case "warning":
		return SeverityWarning, nil
	case "info":
		return SeverityInfo, nil
	default:
		return 0, fmt.Errorf("invalid value for %s: %s (expected error, warning, or info)", key, value)
	}
}

// canonicalType returns a comparable string representation of a column's full
// type, including signedness, length, precision, and charset/binary distinction.
func canonicalType(col *statement.Column) string {
	if col.Raw != nil && col.Raw.Tp != nil {
		return col.Raw.Tp.InfoSchemaStr()
	}
	return col.Type
}

func (l *TypePedanticLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	if l.ignoreColumns == nil {
		if err := l.Configure(l.DefaultConfig()); err != nil {
			panic(err)
		}
	}

	// Collect all CREATE TABLE statements in deterministic order.
	var tables []*statement.CreateTable
	for ct := range CreateTableStatements(existingTables, changes) {
		tables = append(tables, ct)
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})

	tableByName := make(map[string]*statement.CreateTable, len(tables))
	for _, t := range tables {
		tableByName[strings.ToLower(t.TableName)] = t
	}

	if l.checkSameName {
		violations = append(violations, l.lintSameName(tables)...)
	}
	if l.checkInferredFK {
		violations = append(violations, l.lintInferredFK(tables, tableByName)...)
	}

	return violations
}

type colRef struct {
	table *statement.CreateTable
	col   *statement.Column
	typ   string
}

func (l *TypePedanticLinter) lintSameName(tables []*statement.CreateTable) []Violation {
	// Per-table set of indexed column names (lowercased). Built once so each
	// column lookup is O(1). Includes columns from any index — PRIMARY, UNIQUE,
	// secondary, and any position within composite keys — since join optimizers
	// can use any of those for type-sensitive lookups.
	indexedByTable := make(map[*statement.CreateTable]map[string]struct{}, len(tables))
	if l.requireIndexed {
		for _, t := range tables {
			indexedByTable[t] = collectIndexedColumns(t)
		}
	}

	byName := make(map[string][]colRef)
	hasIndexed := make(map[string]bool)
	for _, t := range tables {
		for i := range t.Columns {
			c := &t.Columns[i]
			lower := strings.ToLower(c.Name)
			if _, skip := l.ignoreColumns[lower]; skip {
				continue
			}
			byName[lower] = append(byName[lower], colRef{
				table: t,
				col:   c,
				typ:   canonicalType(c),
			})
			if l.requireIndexed {
				if _, ok := indexedByTable[t][lower]; ok {
					hasIndexed[lower] = true
				}
			}
		}
	}

	// Iterate names in sorted order for deterministic output.
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)

	var violations []Violation
	for _, name := range names {
		refs := byName[name]
		if len(refs) < 2 {
			continue
		}
		if l.requireIndexed && !hasIndexed[name] {
			continue
		}
		typeCounts := make(map[string]int)
		typeTables := make(map[string][]string)
		for _, r := range refs {
			typeCounts[r.typ]++
			typeTables[r.typ] = append(typeTables[r.typ], r.table.TableName)
		}
		if len(typeCounts) == 1 {
			continue
		}
		majority := pickMajorityType(typeCounts)
		majorityTables := dedupeAndSort(typeTables[majority])
		for _, r := range refs {
			if r.typ == majority {
				continue
			}
			colName := r.col.Name
			example := strings.Join(firstN(majorityTables, 3), ", ")
			violations = append(violations, Violation{
				Linter:   l,
				Severity: l.sameNameSeverity,
				Message: fmt.Sprintf(
					"Column %q in table %q has type %q but %d other table(s) use type %q (e.g. %s)",
					r.col.Name, r.table.TableName, r.typ, len(typeTables[majority]), majority, example,
				),
				Location:   &Location{Table: r.table.TableName, Column: &colName},
				Suggestion: strPtr(fmt.Sprintf("Align %s.%s to type %q for consistency", r.table.TableName, r.col.Name, majority)),
				Context: map[string]any{
					"current_type":  r.typ,
					"expected_type": majority,
					"rule":          "same_name",
				},
			})
		}
	}
	return violations
}

func (l *TypePedanticLinter) lintInferredFK(tables []*statement.CreateTable, tableByName map[string]*statement.CreateTable) []Violation {
	var violations []Violation
	for _, t := range tables {
		for i := range t.Columns {
			c := &t.Columns[i]
			lower := strings.ToLower(c.Name)
			if !strings.HasSuffix(lower, "_id") {
				continue
			}
			base := lower[:len(lower)-len("_id")]
			if base == "" {
				continue
			}
			target := findInferredFKTarget(tableByName, base, t.TableName)
			if target == nil {
				continue
			}
			idCol := findIDColumn(target)
			if idCol == nil {
				continue
			}
			colType := canonicalType(c)
			idType := canonicalType(idCol)
			if colType == idType {
				continue
			}
			colName := c.Name
			violations = append(violations, Violation{
				Linter:   l,
				Severity: l.fkSeverity,
				Message: fmt.Sprintf(
					"Column %q in table %q has type %q but inferred FK target %q.id has type %q",
					c.Name, t.TableName, colType, target.TableName, idType,
				),
				Location:   &Location{Table: t.TableName, Column: &colName},
				Suggestion: strPtr(fmt.Sprintf("Change %s.%s to %q to match %s.id", t.TableName, c.Name, idType, target.TableName)),
				Context: map[string]any{
					"current_type":     colType,
					"expected_type":    idType,
					"referenced_table": target.TableName,
					"rule":             "inferred_fk",
				},
			})
		}
	}
	return violations
}

// findInferredFKTarget tries common pluralization variants of base to locate a
// candidate referenced table. Skips matches that point back at the same table
// (a self-reference would not be flagged by this rule).
func findInferredFKTarget(tables map[string]*statement.CreateTable, base, selfName string) *statement.CreateTable {
	candidates := []string{base}
	if !strings.HasSuffix(base, "s") {
		candidates = append(candidates, base+"s")
		candidates = append(candidates, base+"es")
	}
	if strings.HasSuffix(base, "y") && len(base) > 1 {
		candidates = append(candidates, base[:len(base)-1]+"ies")
	}
	selfLower := strings.ToLower(selfName)
	for _, name := range candidates {
		if name == selfLower {
			continue
		}
		if t, ok := tables[name]; ok {
			return t
		}
	}
	return nil
}

// collectIndexedColumns returns a set (lowercased) of every column that
// participates in any index on the table — primary, unique, or secondary,
// at any position within a composite key.
func collectIndexedColumns(t *statement.CreateTable) map[string]struct{} {
	out := make(map[string]struct{})
	for _, idx := range t.Indexes {
		for _, col := range idx.Columns {
			out[strings.ToLower(col)] = struct{}{}
		}
	}
	return out
}

func findIDColumn(t *statement.CreateTable) *statement.Column {
	for i := range t.Columns {
		if strings.EqualFold(t.Columns[i].Name, "id") {
			return &t.Columns[i]
		}
	}
	return nil
}

func pickMajorityType(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var best string
	bestCount := -1
	for _, k := range keys {
		if counts[k] > bestCount {
			best = k
			bestCount = counts[k]
		}
	}
	return best
}

func dedupeAndSort(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func firstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
