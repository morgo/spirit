package dbconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleTLSConfigDefaults(t *testing.T) {
	config := NewDBConfig()

	// Test TLS defaults
	assert.Empty(t, config.TLSCertificatePath)
	assert.Equal(t, "PREFERRED", config.TLSMode)
}

func TestSimpleIsRDSHost(t *testing.T) {
	tests := []struct {
		host     string
		expected bool
	}{
		// Valid RDS hostnames
		{"myhost.us-west-2.rds.amazonaws.com", true},
		{"myhost.us-west-2.rds.amazonaws.com:3306", true},

		// Security test cases - subdomain spoofing attempts
		{"fake-rds.amazonaws.com", false},
		{"evil-rds.amazonaws.com", false},
		{"malicious-rds.amazonaws.com:3306", false},

		// Invalid hostnames
		{"myhost.example.com", false},
		{"localhost", false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			result := IsRDSHost(tt.host)
			assert.Equal(t, tt.expected, result)
		})
	}
}
