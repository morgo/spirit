package migration

import (
	"strings"

	"github.com/go-sql-driver/mysql"
)

// splitReplicaDSNs splits a comma-separated list of replica DSNs.
// A single DSN returns a single-element slice. Empty string returns nil.
func splitReplicaDSNs(dsnList string) []string {
	if dsnList == "" {
		return nil
	}
	parts := strings.Split(dsnList, ",")
	dsns := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			dsns = append(dsns, trimmed)
		}
	}
	return dsns
}

// maskPasswordInDSN masks the password in any DSN string for safe logging
func maskPasswordInDSN(dsn string) string {
	if dsn == "" {
		return dsn
	}

	// Use MySQL driver's ParseDSN for robust parsing
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// If parsing fails, fall back to the original DSN
		// This preserves the original behavior for malformed DSNs
		return dsn
	}

	// Check if the original DSN had a password field by looking for `:` before `@`
	// This handles both empty passwords (user:@host) and non-empty passwords (user:pass@host)
	atIndex := strings.Index(dsn, "@")
	colonIndex := strings.Index(dsn, ":")
	hasPasswordField := colonIndex != -1 && atIndex != -1 && colonIndex < atIndex

	// Only mask if there was actually a password field in the original DSN
	if hasPasswordField {
		cfg.Passwd = "***"
	}

	return cfg.FormatDSN()
}
