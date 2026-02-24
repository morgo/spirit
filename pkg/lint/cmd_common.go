package lint

import (
	"fmt"
	"sort"
)

// sortViolations returns a sorted copy of violations: by table name, then
// severity (errors first), then linter name.
func sortViolations(violations []Violation) []Violation {
	sorted := make([]Violation, len(violations))
	copy(sorted, violations)

	sort.Slice(sorted, func(i, j int) bool {
		ti, tj := "", ""
		if sorted[i].Location != nil {
			ti = sorted[i].Location.Table
		}
		if sorted[j].Location != nil {
			tj = sorted[j].Location.Table
		}
		if ti != tj {
			return ti < tj
		}
		if sorted[i].Severity != sorted[j].Severity {
			return sorted[i].Severity > sorted[j].Severity // errors first
		}
		return sorted[i].Linter.Name() < sorted[j].Linter.Name()
	})

	return sorted
}

// printViolations prints violations to stdout, sorted by table name then severity.
// Used by the lint command which outputs plain text.
func printViolations(violations []Violation) {
	if len(violations) == 0 {
		return
	}

	for _, v := range sortViolations(violations) {
		fmt.Println(v.String())
	}
}
