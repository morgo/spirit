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
// {table}.id and should match its type — JOINs across mismatched types force
// implicit casts and prevent index use.
//
// Both rules operate on a synthesized post-state view: existing tables with
// pending CREATE TABLE / ALTER TABLE changes applied. This makes the linter
// useful both for whole-schema audits and for ALTER-driven migration flows.
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

// setDefaults restores all fields to their default values. Used both as the
// fallback when Lint is called before Configure, and as the prelude inside
// Configure so partial-config calls don't leave stale state from a previous
// configuration.
func (l *TypePedanticLinter) setDefaults() {
	l.checkSameName = true
	l.checkInferredFK = true
	l.requireIndexed = true
	l.ignoreColumns = map[string]struct{}{"id": {}}
	l.fkSeverity = SeverityError
	l.sameNameSeverity = SeverityWarning
}

func (l *TypePedanticLinter) Configure(config map[string]string) error {
	// Always start from defaults so a partial-config call produces the same
	// state as a full-config call with only the overridden keys.
	l.setDefaults()
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
			l.ignoreColumns = tpParseIgnoreList(v)
		case "fkSeverity":
			sev, err := tpParseSeverity(v, k)
			if err != nil {
				return err
			}
			l.fkSeverity = sev
		case "sameNameSeverity":
			sev, err := tpParseSeverity(v, k)
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

func tpParseIgnoreList(value string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, c := range strings.Split(value, ",") {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			out[c] = struct{}{}
		}
	}
	return out
}

func tpParseSeverity(value, key string) (Severity, error) {
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

// tpCanonicalType returns a comparable string representation of a column's
// full type — type name, length, precision, signedness, charset/binary.
func tpCanonicalType(col *statement.Column) string {
	if col.Raw != nil && col.Raw.Tp != nil {
		return col.Raw.Tp.InfoSchemaStr()
	}
	return col.Type
}

func (l *TypePedanticLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	if l.ignoreColumns == nil {
		l.setDefaults()
	}

	tables := PostState(existingTables, changes)
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

// tpCollectIndexedColumns returns the lower-cased set of every column that
// participates in any index on the table. Uses GetIndexes() so that inline
// column-level PRIMARY KEY and UNIQUE declarations are honored — those don't
// appear in the raw Indexes slice.
func tpCollectIndexedColumns(t *statement.CreateTable) map[string]struct{} {
	out := make(map[string]struct{})
	for _, idx := range t.GetIndexes() {
		for _, col := range idx.Columns {
			out[strings.ToLower(col)] = struct{}{}
		}
	}
	return out
}

type tpColRef struct {
	table *statement.CreateTable
	col   *statement.Column
	typ   string
}

func (l *TypePedanticLinter) lintSameName(tables []*statement.CreateTable) []Violation {
	// Precompute per-table indexed-column sets keyed by lower-cased table name.
	indexedByTable := make(map[string]map[string]struct{}, len(tables))
	if l.requireIndexed {
		for _, t := range tables {
			indexedByTable[strings.ToLower(t.TableName)] = tpCollectIndexedColumns(t)
		}
	}

	byName := make(map[string][]tpColRef)
	hasIndexed := make(map[string]bool)
	for _, t := range tables {
		tLower := strings.ToLower(t.TableName)
		for i := range t.Columns {
			c := &t.Columns[i]
			lower := strings.ToLower(c.Name)
			if _, skip := l.ignoreColumns[lower]; skip {
				continue
			}
			byName[lower] = append(byName[lower], tpColRef{
				table: t,
				col:   c,
				typ:   tpCanonicalType(c),
			})
			if l.requireIndexed {
				if _, ok := indexedByTable[tLower][lower]; ok {
					hasIndexed[lower] = true
				}
			}
		}
	}

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

		majority, clear := tpPickMajority(typeCounts)
		if clear {
			majorityTables := tpDedupeStrings(typeTables[majority])
			for _, r := range refs {
				if r.typ == majority {
					continue
				}
				colName := r.col.Name
				example := strings.Join(tpFirstN(majorityTables, 3), ", ")
				violations = append(violations, Violation{
					Linter:   l,
					Severity: l.sameNameSeverity,
					Message: fmt.Sprintf(
						"Column %q in table %q has type %q but %d other table(s) use type %q (e.g. %s)",
						r.col.Name, r.table.TableName, r.typ, len(majorityTables), majority, example,
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
		} else {
			// Tied top counts — no canonical "right" type. Report every occurrence
			// as inconsistent, listing the conflicting types so the user can decide.
			distinct := make([]string, 0, len(typeCounts))
			for tp := range typeCounts {
				distinct = append(distinct, tp)
			}
			sort.Strings(distinct)
			for _, r := range refs {
				colName := r.col.Name
				violations = append(violations, Violation{
					Linter:   l,
					Severity: l.sameNameSeverity,
					Message: fmt.Sprintf(
						"Column %q in table %q has type %q; inconsistent across schema (types in use: %s)",
						r.col.Name, r.table.TableName, r.typ, strings.Join(distinct, ", "),
					),
					Location:   &Location{Table: r.table.TableName, Column: &colName},
					Suggestion: strPtr(fmt.Sprintf("Pick one canonical type for column %q across all tables; the larger/safer type is usually right", r.col.Name)),
					Context: map[string]any{
						"current_type":      r.typ,
						"conflicting_types": distinct,
						"rule":              "same_name",
					},
				})
			}
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
			if _, skip := l.ignoreColumns[lower]; skip {
				continue
			}
			if !strings.HasSuffix(lower, "_id") {
				continue
			}
			base := lower[:len(lower)-len("_id")]
			if base == "" {
				continue
			}
			target := tpFindFKTarget(tableByName, base, t.TableName)
			if target == nil {
				continue
			}
			idCol := tpFindIDColumn(target)
			if idCol == nil {
				continue
			}
			colType := tpCanonicalType(c)
			idType := tpCanonicalType(idCol)
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
				Location: &Location{Table: t.TableName, Column: &colName},
				Suggestion: strPtr(fmt.Sprintf(
					"Align types: %s.%s (%s) and %s.id (%s) should match — grow the smaller side rather than shrink the larger",
					t.TableName, c.Name, colType, target.TableName, idType,
				)),
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

// tpFindFKTarget tries common pluralization variants of base to locate a
// candidate referenced table. Skips self-references.
func tpFindFKTarget(tables map[string]*statement.CreateTable, base, selfName string) *statement.CreateTable {
	selfLower := strings.ToLower(selfName)
	for _, name := range tpPluralCandidates(base) {
		if name == selfLower {
			continue
		}
		if t, ok := tables[name]; ok {
			return t
		}
	}
	return nil
}

// tpPluralCandidates returns plausible table-name forms for an FK base.
// Order matters: the literal base comes first, then +s, then +es, then y→ies.
// This covers:
//
//	customer    → [customer, customers]
//	address     → [address, addresses]            (s-stem: +es)
//	process     → [process, processes]            (s-stem: +es)
//	bus         → [bus, buses]                    (s-stem: +es)
//	box         → [box, boxs, boxes]              (x-stem: +es; boxs is harmless noise)
//	tomato      → [tomato, tomatos, tomatoes]     (o-stem: +es)
//	category    → [category, categorys, categories] (y-stem: +ies)
//	city        → [city, citys, cities]
func tpPluralCandidates(base string) []string {
	if base == "" {
		return nil
	}
	out := []string{base}
	if !strings.HasSuffix(base, "s") {
		out = append(out, base+"s")
	}
	switch {
	case strings.HasSuffix(base, "s"),
		strings.HasSuffix(base, "x"),
		strings.HasSuffix(base, "z"),
		strings.HasSuffix(base, "ch"),
		strings.HasSuffix(base, "sh"),
		strings.HasSuffix(base, "o"):
		out = append(out, base+"es")
	}
	if strings.HasSuffix(base, "y") && len(base) > 1 {
		out = append(out, base[:len(base)-1]+"ies")
	}
	return out
}

func tpFindIDColumn(t *statement.CreateTable) *statement.Column {
	for i := range t.Columns {
		if strings.EqualFold(t.Columns[i].Name, "id") {
			return &t.Columns[i]
		}
	}
	return nil
}

// tpPickMajority returns (winningType, true) when one type strictly dominates,
// or ("", false) when the top count is tied between two or more types.
func tpPickMajority(counts map[string]int) (string, bool) {
	if len(counts) == 0 {
		return "", false
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var first string
	firstCount, secondCount := -1, -1
	for _, k := range keys {
		c := counts[k]
		switch {
		case c > firstCount:
			secondCount = firstCount
			firstCount = c
			first = k
		case c > secondCount:
			secondCount = c
		}
	}
	if firstCount > secondCount {
		return first, true
	}
	return "", false
}

func tpDedupeStrings(ss []string) []string {
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

func tpFirstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
