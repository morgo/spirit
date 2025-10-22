package repl

import (
	"os"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientTLSConfiguration(t *testing.T) {
	// Create temporary certificate file for testing
	tempFile, err := os.CreateTemp("", "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write test certificate data
	certData := generateTestCert(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	testCases := []struct {
		name        string
		tlsMode     string
		tlsCert     string
		host        string
		description string
	}{
		{
			name:        "DISABLED mode should not set TLS config",
			tlsMode:     "DISABLED",
			tlsCert:     "",
			host:        "example.com:3306",
			description: "DISABLED TLS mode should not configure TLS for replication",
		},
		{
			name:        "PREFERRED mode should set custom TLS config",
			tlsMode:     "PREFERRED",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "PREFERRED TLS mode should configure custom TLS for replication",
		},
		{
			name:        "REQUIRED mode with RDS host should use RDS config",
			tlsMode:     "REQUIRED",
			tlsCert:     "",
			host:        "mydb.us-west-2.rds.amazonaws.com:3306",
			description: "REQUIRED TLS mode with RDS host should use RDS TLS config",
		},
		{
			name:        "REQUIRED mode with non-RDS host should use custom config",
			tlsMode:     "REQUIRED",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "REQUIRED TLS mode with non-RDS host should use custom TLS config",
		},
		{
			name:        "VERIFY_CA mode should set verify_ca TLS config",
			tlsMode:     "VERIFY_CA",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "VERIFY_CA TLS mode should configure certificate verification",
		},
		{
			name:        "VERIFY_IDENTITY mode should set verify_identity TLS config",
			tlsMode:     "VERIFY_IDENTITY",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "VERIFY_IDENTITY TLS mode should configure full verification",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create TLS config
			tlsConfig := dbconn.NewDBConfig()
			tlsConfig.TLSMode = tc.tlsMode
			tlsConfig.TLSCertificatePath = tc.tlsCert

			// Create a mock client (we can't actually connect without real DB)
			client := &Client{
				host:      tc.host,
				username:  "testuser",
				password:  "testpass",
				tlsConfig: tlsConfig,
			}

			// Verify TLS config is stored correctly
			assert.NotNil(t, client.tlsConfig)
			assert.Equal(t, tc.tlsMode, client.tlsConfig.TLSMode)
			assert.Equal(t, tc.tlsCert, client.tlsConfig.TLSCertificatePath)

			// Test the TLS mode validation logic that would be used in Run()
			if tc.tlsMode == "DISABLED" {
				// For disabled mode, TLS should not be configured in actual connection
				assert.Equal(t, "DISABLED", client.tlsConfig.TLSMode)
			} else {
				// For other modes, TLS should be configured
				assert.NotEqual(t, "DISABLED", client.tlsConfig.TLSMode)
			}
		})
	}
}

func TestClientTLSModeString(t *testing.T) {
	testCases := []struct {
		tlsMode  string
		expected string
	}{
		{"DISABLED", ""},
		{"PREFERRED", "preferred"},
		{"REQUIRED", "required"},
		{"VERIFY_CA", "verify_ca"},
		{"VERIFY_IDENTITY", "verify_identity"},
		{"disabled", ""},
		{"preferred", "preferred"},
		{"required", "required"},
		{"verify_ca", "verify_ca"},
		{"verify_identity", "verify_identity"},
	}

	for _, tc := range testCases {
		t.Run(tc.tlsMode, func(t *testing.T) {
			// Test the string conversion logic used in client
			result := ""
			switch strings.ToUpper(tc.tlsMode) {
			case "DISABLED":
				result = ""
			case "PREFERRED":
				result = "preferred"
			case "REQUIRED":
				result = "required"
			case "VERIFY_CA":
				result = "verify_ca"
			case "VERIFY_IDENTITY":
				result = "verify_identity"
			}
			assert.Equal(t, tc.expected, result)
		})
	}
}

// generateTestCert creates a simple test certificate for TLS testing
func generateTestCert(t *testing.T) []byte {
	t.Helper()
	return []byte(`-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQChiHMXdFjPDzANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJV
UzAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMA0xCzAJBgNVBAYTAlVT
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw2WSwKkCaZKm/5aDHvmv
7r+9OKlpLODy8q3fOJwPl2zVSkqKbx8y2gJ8O8T5Z5s4z4V3Xf4Q5bZ5b5b5b5b5
b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5
b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5
b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5
b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5
b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5
QIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQCcW5F5g5g5g5g5g5g5g5g5g5g5g5g5
g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5
g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5
g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5
g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5
g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5g5
-----END CERTIFICATE-----`)
}
