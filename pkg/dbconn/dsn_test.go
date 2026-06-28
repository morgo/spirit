package dbconn

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic DSN with password", "user:password@tcp(localhost:3306)/database", "user:***@tcp(localhost:3306)/database"},
		{"DSN with complex password", "myuser:c0mplex!Pa$$w0rd@tcp(db.example.com:3306)/mydb", "myuser:***@tcp(db.example.com:3306)/mydb"},
		{"DSN without password", "user@tcp(localhost:3306)/database", "user@tcp(localhost:3306)/database"},
		{"DSN with empty password", "user:@tcp(localhost:3306)/database", "user:***@tcp(localhost:3306)/database"},
		{"empty DSN", "", ""},
		{"malformed DSN without @", "user:password", "user:password"},
		{"DSN with colon in password", "user:pass:word@tcp(localhost:3306)/database", "user:***@tcp(localhost:3306)/database"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, RedactDSN(tc.input))
		})
	}
}

func TestSplitDSNs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"empty", "", nil},
		{"single DSN", "root:pass@tcp(localhost:3307)/test", []string{"root:pass@tcp(localhost:3307)/test"}},
		{
			name:     "two DSNs",
			input:    "root:pass@tcp(replica1:3306)/db,root:pass@tcp(replica2:3306)/db",
			expected: []string{"root:pass@tcp(replica1:3306)/db", "root:pass@tcp(replica2:3306)/db"},
		},
		{
			name:     "three DSNs with spaces",
			input:    "root:pass@tcp(r1:3306)/db, root:pass@tcp(r2:3306)/db , root:pass@tcp(r3:3306)/db",
			expected: []string{"root:pass@tcp(r1:3306)/db", "root:pass@tcp(r2:3306)/db", "root:pass@tcp(r3:3306)/db"},
		},
		{"trailing comma", "root:pass@tcp(localhost:3306)/db,", []string{"root:pass@tcp(localhost:3306)/db"}},
		{"only commas and spaces", " , , , ", []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, SplitDSNs(tc.input))
		})
	}
}
