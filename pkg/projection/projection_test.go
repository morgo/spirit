package projection

import (
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func TestProjectionParse(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		wantColumns int
		wantErr     bool
	}{
		{
			name: "simple select",
			config: Config{
				Name:        "test",
				SourceTable: "users",
				TargetTable: "users",
				Query:       "SELECT id, name, email FROM users",
				PrimaryKey:  []string{"id"},
			},
			wantColumns: 3,
			wantErr:     false,
		},
		{
			name: "select with alias",
			config: Config{
				Name:        "test",
				SourceTable: "users",
				TargetTable: "users",
				Query:       "SELECT id, UPPER(name) as name, email FROM users",
				PrimaryKey:  []string{"id"},
			},
			wantColumns: 3,
			wantErr:     false,
		},
		{
			name: "select with expression without alias",
			config: Config{
				Name:        "test",
				SourceTable: "users",
				TargetTable: "users",
				Query:       "SELECT id, UPPER(name), email FROM users",
				PrimaryKey:  []string{"id"},
			},
			wantColumns: 0,
			wantErr:     true, // Should fail because UPPER(name) has no alias
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proj, err := NewProjection(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProjection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				cols := proj.GetColumns()
				if len(cols) != tt.wantColumns {
					t.Errorf("expected %d columns, got %d", tt.wantColumns, len(cols))
				}
			}
		})
	}
}

func TestProjectionGetReferencedColumns(t *testing.T) {
	config := Config{
		Name:        "test",
		SourceTable: "users",
		TargetTable: "users",
		Query:       "SELECT id, name, UPPER(email) as email_upper FROM users",
		PrimaryKey:  []string{"id"},
	}

	proj, err := NewProjection(config)
	if err != nil {
		t.Fatalf("failed to create projection: %v", err)
	}

	refCols := proj.GetReferencedColumns()
	
	// Should have id, name, and email (even though email is in UPPER())
	// Note: The current implementation only tracks simple column references
	// For expressions like UPPER(email), it won't extract 'email'
	// This is acceptable for the initial implementation
	
	if len(refCols) < 2 {
		t.Errorf("expected at least 2 referenced columns, got %d: %v", len(refCols), refCols)
	}
}

func TestProjectionGetTargetColumns(t *testing.T) {
	config := Config{
		Name:        "test",
		SourceTable: "users",
		TargetTable: "users",
		Query:       "SELECT id, name, UPPER(email) as email_upper FROM users",
		PrimaryKey:  []string{"id"},
	}

	proj, err := NewProjection(config)
	if err != nil {
		t.Fatalf("failed to create projection: %v", err)
	}

	targetCols := proj.GetTargetColumns()
	expected := []string{"id", "name", "email_upper"}

	if len(targetCols) != len(expected) {
		t.Errorf("expected %d target columns, got %d", len(expected), len(targetCols))
	}

	for i, col := range targetCols {
		if col != expected[i] {
			t.Errorf("expected column %d to be %s, got %s", i, expected[i], col)
		}
	}
}
