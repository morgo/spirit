package utils

import (
	"regexp"
	"strings"
)

// dbGrantRegexp captures the privilege list and database-name pattern from a
// database-level grant line, e.g.
//
//	GRANT ALL PRIVILEGES ON `strata_%`.* TO `user`@`%`
//
// capturing "ALL PRIVILEGES" and "strata_%". It only matches database-level
// grants (`db`.*); global (*.*), table-level, and routine grants do not match.
var dbGrantRegexp = regexp.MustCompile("^GRANT (.+) ON `([^`]+)`\\.\\* TO ")

// migrationDBPrivileges is the database-level privilege set spirit requires to
// run a migration or move (mirroring gh-ost's historical requirement). A grant
// of ALL PRIVILEGES, or of every privilege in this set, satisfies the check.
var migrationDBPrivileges = []string{
	"ALTER", "CREATE", "DELETE", "DROP", "INDEX", "INSERT",
	"LOCK TABLES", "SELECT", "TRIGGER", "UPDATE",
}

// DBLevelGrantCoversSchema reports whether a single SHOW GRANTS line is a
// database-level grant that confers the privileges spirit needs on schemaName.
// Unlike a literal substring match, it expands MySQL wildcard patterns in the
// granted database name (see MySQLLikeMatch), so a grant on `strata_%`.* is
// recognized as covering strata_boardgames_sharded_n80. The previous literal
// match handled only exact and escaped-underscore database names.
func DBLevelGrantCoversSchema(grant, schemaName string) bool {
	m := dbGrantRegexp.FindStringSubmatch(grant)
	if m == nil {
		return false
	}
	privs, dbPattern := m[1], m[2]
	if !MySQLLikeMatch(dbPattern, schemaName) {
		return false
	}
	if strings.Contains(privs, "ALL PRIVILEGES") {
		return true
	}
	for _, p := range migrationDBPrivileges {
		if !strings.Contains(privs, p) {
			return false
		}
	}
	return true
}

// MySQLLikeMatch reports whether name matches the given MySQL LIKE-style
// pattern. As in MySQL pattern matching, '%' matches any sequence of
// characters (including the empty string), '_' matches any single character,
// and a backslash escapes the following character so that it is treated as a
// literal (so `\%` and `\_` match a literal '%' and '_').
//
// This mirrors how MySQL evaluates the database-name portion of a
// database-level GRANT. A grant on `strata_%`.* applies to a database named
// strata_boardgames, even though SHOW GRANTS reports the pattern verbatim.
// Privilege checks therefore cannot compare the granted database name to the
// target schema with a plain string match; they must expand the pattern.
func MySQLLikeMatch(pattern, name string) bool {
	p, s := []byte(pattern), []byte(name)
	var pi, si int
	// Position of the most recent '%' in the pattern and the input position
	// where it began matching, so we can backtrack and let it consume one
	// more character when a later literal fails.
	starPi, starSi := -1, 0
	for si < len(s) {
		if pi < len(p) {
			switch c := p[pi]; {
			case c == '\\' && pi+1 < len(p):
				if p[pi+1] == s[si] {
					pi += 2
					si++
					continue
				}
			case c == '%':
				starPi, starSi = pi, si
				pi++
				continue
			case c == '_' || c == s[si]:
				pi++
				si++
				continue
			}
		}
		if starPi != -1 {
			// Backtrack: let the '%' absorb one more character.
			pi = starPi + 1
			starSi++
			si = starSi
			continue
		}
		return false
	}
	// Any pattern remaining must be all '%' to match the empty suffix.
	for pi < len(p) && p[pi] == '%' {
		pi++
	}
	return pi == len(p)
}
