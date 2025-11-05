package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
)

func TestRedundantIndexLinter_Name(t *testing.T) {
	linter := &RedundantIndexLinter{}
	assert.Equal(t, "redundant_indexes", linter.Name())
}

func TestRedundantIndexLinter_Description(t *testing.T) {
	linter := &RedundantIndexLinter{}
	assert.NotEmpty(t, linter.Description())
}

func TestRedundantIndexLinter_PrefixRedundancy(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
		coveringIndex  string
	}{
		{
			name: "index (a) redundant to index (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a),
				INDEX idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			coveringIndex:  "idx_ab",
		},
		{
			name: "index (a, b) redundant to index (a, b, c)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				c INT,
				INDEX idx_ab (a, b),
				INDEX idx_abc (a, b, c)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_ab",
			coveringIndex:  "idx_abc",
		},
		{
			name: "index (a, b) NOT redundant to index (a, c)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				c INT,
				INDEX idx_ab (a, b),
				INDEX idx_ac (a, c)
			)`,
			expectViolated: false,
		},
		{
			name: "index (a, b) NOT redundant to index (b, a)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_ab (a, b),
				INDEX idx_ba (b, a)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			assert.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				assert.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						assert.Contains(t, v.Message, tt.coveringIndex)
						assert.Equal(t, SeverityWarning, v.Severity)
						break
					}
				}
				assert.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				assert.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

func TestRedundantIndexLinter_DuplicateIndexes(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
	}{
		{
			name: "duplicate indexes (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx1 (a, b),
				INDEX idx2 (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx2",
		},
		{
			name: "duplicate single column indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				INDEX idx1 (a),
				INDEX idx2 (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx2",
		},
		{
			name: "duplicate UNIQUE indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE idx1 (a, b),
				UNIQUE idx2 (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			assert.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				assert.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						assert.Contains(t, v.Message, "duplicate")
						assert.Equal(t, SeverityWarning, v.Severity)
						break
					}
				}
				assert.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				assert.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

func TestRedundantIndexLinter_RedundantToPrimaryKey(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
		isDuplicate    bool
	}{
		{
			name: "index (a) redundant to PK (a)",
			createTable: `CREATE TABLE t1 (
				a INT PRIMARY KEY,
				INDEX idx_a (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			isDuplicate:    true,
		},
		{
			name: "index (a) redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx_a (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			isDuplicate:    false,
		},
		{
			name: "index (a, b) redundant to PK (a, b, c)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b, c),
				INDEX idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_ab",
			isDuplicate:    false,
		},
		{
			name: "index (a, c) NOT redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx_ac (a, c)
			)`,
			expectViolated: false,
		},
		{
			name: "index (b) NOT redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx_b (b)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			assert.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				assert.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						assert.Contains(t, v.Message, "PRIMARY KEY")
						assert.Equal(t, SeverityWarning, v.Severity)
						if tt.isDuplicate {
							assert.Contains(t, v.Message, "duplicate")
						}
						break
					}
				}
				assert.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				assert.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

func TestRedundantIndexLinter_PKSuffixRedundancy(t *testing.T) {
	tests := []struct {
		name              string
		createTable       string
		expectViolated    bool
		violatedIndex     string
		redundantColCount int
	}{
		{
			name: "index (b, c, a) has redundant PK suffix (a)",
			createTable: `CREATE TABLE t1 (
				a INT PRIMARY KEY,
				b INT,
				c INT,
				INDEX idx (b, c, a)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 1,
		},
		{
			name: "index (c, a, b) has redundant PK suffix (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, a, b)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 2,
		},
		{
			name: "index (c, d, a) has redundant PK prefix suffix (a) when PK is (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				d INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, d, a)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 1,
		},
		{
			name: "index (x, y, a, b, c) has redundant PK suffix (a, b, c)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				x INT,
				y INT,
				PRIMARY KEY (a, b, c),
				INDEX idx (x, y, a, b, c)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 3,
		},
		{
			name: "index (c, b) does NOT have redundant PK suffix when PK is (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, b)
			)`,
			expectViolated: false,
		},
		{
			name: "index (c, b, a) does NOT have redundant PK suffix when PK is (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, b, a)
			)`,
			expectViolated: false,
		},
		{
			name: "index (a, b) does NOT have suffix when it's same length as PK",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx (a, b)
			)`,
			expectViolated: false, // This is a duplicate, not a suffix issue
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			assert.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						if v.Context != nil {
							if colCount, ok := v.Context["redundant_col_count"]; ok && colCount == tt.redundantColCount {
								found = true
								assert.Contains(t, v.Message, "redundant PRIMARY KEY")
								assert.Contains(t, v.Message, "suffix")
								assert.Equal(t, SeverityWarning, v.Severity)
								break
							}
						}
					}
				}
				assert.True(t, found, "Expected PK suffix violation for index %s with %d redundant columns", tt.violatedIndex, tt.redundantColCount)
			} else {
				// Check that there's no PK suffix violation for this index
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						assert.NotContains(t, v.Message, "suffix", "Should not have suffix violation")
					}
				}
			}
		})
	}
}

func TestRedundantIndexLinter_TypeCompatibility(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
	}{
		{
			name: "INDEX (a) redundant to UNIQUE (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a),
				UNIQUE idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
		},
		{
			name: "UNIQUE (a) NOT redundant to INDEX (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE idx_a (a),
				INDEX idx_ab (a, b)
			)`,
			expectViolated: false,
		},
		{
			name: "UNIQUE (a) redundant to UNIQUE (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE idx_a (a),
				UNIQUE idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
		},
		{
			name: "UNIQUE (a) redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				UNIQUE idx_a (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
		},
		{
			name: "FULLTEXT index NOT considered redundant",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				title TEXT,
				INDEX idx_title (title(100)),
				FULLTEXT idx_ft (title)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			assert.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil {
						assert.NotEqual(t, tt.violatedIndex, *v.Location.Index, "Should not have violation for %s", tt.violatedIndex)
					}
				}
			}
		})
	}
}

func TestRedundantIndexLinter_MultipleViolations(t *testing.T) {
	createTable := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		a INT,
		b INT,
		c INT,
		INDEX idx_a (a),
		INDEX idx_ab (a, b),
		INDEX idx_abc (a, b, c),
		INDEX idx_dup (a, b),
		INDEX idx_suffix (c, id)
	)`

	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	assert.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should detect:
	// 1. idx_a redundant to idx_ab
	// 2. idx_ab redundant to idx_abc
	// 3. idx_dup redundant to idx_abc (or idx_ab)
	// 4. idx_suffix has redundant PK suffix
	assert.GreaterOrEqual(t, len(violations), 4, "Expected at least 4 violations")

	violatedIndexes := make(map[string]bool)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			violatedIndexes[*v.Location.Index] = true
		}
	}

	assert.True(t, violatedIndexes["idx_a"], "idx_a should be flagged")
	assert.True(t, violatedIndexes["idx_ab"], "idx_ab should be flagged")
	assert.True(t, violatedIndexes["idx_dup"], "idx_dup should be flagged")
	assert.True(t, violatedIndexes["idx_suffix"], "idx_suffix should be flagged")
}

func TestRedundantIndexLinter_NoViolations(t *testing.T) {
	tests := []struct {
		name        string
		createTable string
	}{
		{
			name: "no redundant indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				c INT,
				INDEX idx_a (a),
				INDEX idx_b (b),
				INDEX idx_c (c)
			)`,
		},
		{
			name: "different column orders",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_ab (a, b),
				INDEX idx_ba (b, a)
			)`,
		},
		{
			name: "no indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT
			)`,
		},
		{
			name: "only primary key",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY
			)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			assert.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)
			assert.Empty(t, violations, "Expected no violations but got: %v", violations)
		})
	}
}

func TestRedundantIndexLinter_CompositePrimaryKey(t *testing.T) {
	createTable := `CREATE TABLE t1 (
		tenant_id INT,
		user_id INT,
		email VARCHAR(255),
		created_at TIMESTAMP,
		PRIMARY KEY (tenant_id, user_id),
		INDEX idx_tenant (tenant_id),
		INDEX idx_email (email),
		INDEX idx_email_tenant_user (email, tenant_id, user_id),
		INDEX idx_created (created_at, tenant_id, user_id)
	)`

	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	assert.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	violatedIndexes := make(map[string]string)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			violatedIndexes[*v.Location.Index] = v.Message
		}
	}

	// idx_tenant should be redundant to PK (tenant_id, user_id)
	assert.Contains(t, violatedIndexes, "idx_tenant", "idx_tenant should be redundant to PK")

	// idx_email_tenant_user should have redundant PK suffix
	assert.Contains(t, violatedIndexes, "idx_email_tenant_user", "idx_email_tenant_user should have redundant suffix")
	assert.Contains(t, violatedIndexes["idx_email_tenant_user"], "suffix")

	// idx_email should be redundant to idx_email_tenant_user (prefix match)
	assert.Contains(t, violatedIndexes, "idx_email", "idx_email should be redundant to idx_email_tenant_user")

	// idx_created should have redundant PK suffix (tenant_id, user_id)
	assert.Contains(t, violatedIndexes, "idx_created", "idx_created should have redundant suffix")
	assert.Contains(t, violatedIndexes["idx_created"], "suffix")
}
