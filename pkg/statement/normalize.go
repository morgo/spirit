package statement

// This file provides a registry of "normalization" rules. A normalization
// rewrites a parsed CreateTable so that a human-authored schema and a live
// SHOW CREATE TABLE converge to the same structured form before Diff compares
// them.
//
// MySQL rewrites a great many constructs when it stores a table definition.
// Some of those rewrites are already absorbed by the parser (BOOLEAN and BOOL
// arrive as tinyint(1), SERIAL as bigint unsigned auto_increment unique,
// INTEGER as int, NVARCHAR as varchar, ...). The ones that survive into the
// structured form are handled here: each is a Normalizer registered from an
// init() in its own normalize_*.go file, mirroring how pkg/migration/check
// registers checks. Contributors add a MySQL behavior by dropping in a new
// file — no change to Diff or the parser is required.
//
// Rules run at the tail of parseToStruct, after every field (columns, indexes,
// constraints, table options, partitions) is populated. Because of that, a
// rule can rely on the whole struct being present and rules do not depend on
// each other's ordering; keep them order-independent and idempotent.

// Normalizer applies a single MySQL canonicalization to a parsed CreateTable.
// A rule takes a CreateTable and returns the normalized CreateTable — it is
// free to mutate and return the same instance, or to return a new one. It is
// deliberately a standalone type rather than a method on CreateTable so rules
// live in their own files and compose as a pipeline.
type Normalizer interface {
	// Name identifies the rule, for registry determinism and debugging.
	Name() string
	// Normalize returns ct rewritten to MySQL's canonical form for this rule.
	Normalize(ct *CreateTable) *CreateTable
}

// normalizers holds every registered rule, in registration order. Order is not
// significant (see the package comment); it is preserved only so output is
// deterministic.
var normalizers []Normalizer

// registerNormalizer adds a rule to the registry. It is intended to be called
// from an init() function so rules self-register at package load.
func registerNormalizer(n Normalizer) {
	normalizers = append(normalizers, n)
}

// runNormalizers applies every registered Normalizer to ct, threading the
// result of each into the next, and returns the final CreateTable.
func runNormalizers(ct *CreateTable) *CreateTable {
	for _, n := range normalizers {
		ct = n.Normalize(ct)
	}
	return ct
}
