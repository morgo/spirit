# Spirit Replication TLS Testing Matrix

Docker Compose tests for Spirit's replica TLS support, validating both replica throttler and binary log replication TLS inheritance.

## Quick Start

```bash
# Test standard TLS inheritance (default MySQL certificates)
make cleanup && make start && make verify && make cleanup

# Test full VERIFY_IDENTITY support (custom certificates)
make cleanup && make generate-certs && make start-custom && make verify-custom && make cleanup-custom

# Test mixed TLS environments (main TLS enabled, replica TLS disabled)
make cleanup && make start-mixed && make verify-mixed && make cleanup-mixed
```

## Test Environments

- **Standard TLS** (ports 3400-3401): Both servers with TLS enabled, default certificates
- **Custom TLS** (ports 3410-3411): Both servers with custom certificates supporting VERIFY_IDENTITY  
- **Mixed TLS** (ports 3420-3421): Main server TLS enabled, replica server TLS disabled

## Complete TLS Configuration Matrix

This comprehensive matrix shows all possible combinations of main DB TLS modes and replica DSN configurations, with exact end results for both connections.

### Matrix Legend
- 🟢 **TLS Enabled** - Encrypted connection established
- 🔴 **TLS Disabled** - Plain text connection  
- 🟡 **TLS Conditional** - Depends on server capabilities
- 🔒 **Preserved** - Existing DSN setting maintained
- 🏷️ **RDS Auto** - Automatic RDS certificate detection

### Complete TLS Behavior Matrix

| Main DB CLI Flags | Main DB Result | Replica DSN | Replica Result | Final State | Use Case |
|-------------------|----------------|-------------|----------------|-------------|----------|
| **Default Behavior (PREFERRED)** |
| *(no --tls-mode)* | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db` | 🟡 Inherits PREFERRED | Main: 🟡 PREFERRED<br>Replica: 🟡 PREFERRED | Standard auto-detection |
| *(no --tls-mode)* | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🟡 PREFERRED<br>Replica: 🔴 NO TLS | Performance optimization |
| *(no --tls-mode)* | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db?tls=required` | 🟢 REQUIRED | Main: 🟡 PREFERRED<br>Replica: 🟢 REQUIRED | Explicit replica security |
| *(no --tls-mode)* | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db?tls=skip-verify` | 🟢 skip-verify | Main: 🟡 PREFERRED<br>Replica: 🟢 skip-verify | Mixed security levels |
| *(no --tls-mode)* | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db?tls=preferred` | 🟡 PREFERRED | Main: 🟡 PREFERRED<br>Replica: 🟡 PREFERRED | Explicit conditional |
| **DISABLED Mode** |
| `--tls-mode DISABLED` | 🔴 DISABLED | `user:pass@tcp(replica:3306)/db` | 🔴 NO TLS | Main: 🔴 DISABLED<br>Replica: 🔴 NO TLS | No encryption anywhere |
| `--tls-mode DISABLED` | 🔴 DISABLED | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🔴 DISABLED<br>Replica: 🔴 NO TLS | Explicit no-TLS confirmation |
| `--tls-mode DISABLED` | 🔴 DISABLED | `user:pass@tcp(replica:3306)/db?tls=preferred` | 🟡 PREFERRED | Main: 🔴 DISABLED<br>Replica: 🟡 PREFERRED | Conditional override |
| `--tls-mode DISABLED` | 🔴 DISABLED | `user:pass@tcp(replica:3306)/db?tls=required` | 🟢 REQUIRED | Main: 🔴 DISABLED<br>Replica: 🟢 REQUIRED | Security override |
| **PREFERRED Mode (Explicit)** |
| `--tls-mode PREFERRED` | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db` | 🟡 Inherits PREFERRED | Main: 🟡 PREFERRED<br>Replica: 🟡 PREFERRED | Explicit conditional mode |
| `--tls-mode PREFERRED` | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🟡 PREFERRED<br>Replica: 🔴 NO TLS | Conditional/disabled split |
| `--tls-mode PREFERRED` | 🟡 PREFERRED | `user:pass@tcp(replica:3306)/db?tls=required` | 🟢 REQUIRED | Main: 🟡 PREFERRED<br>Replica: 🟢 REQUIRED | Conditional/required split |
| **REQUIRED Mode** |
| `--tls-mode REQUIRED` | 🟢 REQUIRED | `user:pass@tcp(replica:3306)/db` | 🟢 Inherits REQUIRED | Main: 🟢 REQUIRED<br>Replica: 🟢 REQUIRED | Uniform encryption |
| `--tls-mode REQUIRED` | 🟢 REQUIRED | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🟢 REQUIRED<br>Replica: 🔴 NO TLS | Performance exception |
| `--tls-mode REQUIRED` | 🟢 REQUIRED | `user:pass@tcp(replica:3306)/db?tls=preferred` | � PREFERRED | Main: 🟢 REQUIRED<br>Replica: � PREFERRED | Required/conditional split |
| `--tls-mode REQUIRED` | 🟢 REQUIRED | `user:pass@tcp(replica:3306)/db?tls=skip-verify` | � skip-verify | Main: 🟢 REQUIRED<br>Replica: � skip-verify | Different verification |
| **VERIFY_CA Mode** |
| `--tls-mode VERIFY_CA` | 🟢 VERIFY_CA | `user:pass@tcp(replica:3306)/db` | 🟢 Inherits VERIFY_CA | Main: 🟢 VERIFY_CA<br>Replica: 🟢 VERIFY_CA | CA verification |
| `--tls-mode VERIFY_CA` | 🟢 VERIFY_CA | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🟢 VERIFY_CA<br>Replica: 🔴 NO TLS | Security/performance split |
| `--tls-mode VERIFY_CA` | 🟢 VERIFY_CA | `user:pass@tcp(replica:3306)/db?tls=preferred` | 🟡 PREFERRED | Main: 🟢 VERIFY_CA<br>Replica: 🟡 PREFERRED | CA/conditional split |
| `--tls-mode VERIFY_CA` | 🟢 VERIFY_CA | `user:pass@tcp(replica:3306)/db?tls=required` | 🟢 REQUIRED | Main: 🟢 VERIFY_CA<br>Replica: 🟢 REQUIRED | Downgraded verification |
| `--tls-mode VERIFY_CA` | 🟢 VERIFY_CA | `user:pass@tcp(replica:3306)/db?tls=verify-identity` | 🟢 VERIFY_IDENTITY | Main: 🟢 VERIFY_CA<br>Replica: 🟢 VERIFY_IDENTITY | Upgraded verification |
| **VERIFY_IDENTITY Mode** |
| `--tls-mode VERIFY_IDENTITY` | 🟢 VERIFY_IDENTITY | `user:pass@tcp(replica:3306)/db` | 🟢 Inherits VERIFY_IDENTITY | Main: 🟢 VERIFY_IDENTITY<br>Replica: 🟢 VERIFY_IDENTITY | Maximum security |
| `--tls-mode VERIFY_IDENTITY` | 🟢 VERIFY_IDENTITY | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🟢 VERIFY_IDENTITY<br>Replica: 🔴 NO TLS | High security/fast replica |
| `--tls-mode VERIFY_IDENTITY` | 🟢 VERIFY_IDENTITY | `user:pass@tcp(replica:3306)/db?tls=preferred` | 🟡 PREFERRED | Main: 🟢 VERIFY_IDENTITY<br>Replica: 🟡 PREFERRED | Identity/conditional split |
| `--tls-mode VERIFY_IDENTITY` | 🟢 VERIFY_IDENTITY | `user:pass@tcp(replica:3306)/db?tls=skip-verify` | 🟢 skip-verify | Main: 🟢 VERIFY_IDENTITY<br>Replica: 🟢 skip-verify | Security/compatibility split |
| **Custom Certificate Scenarios** |
| `--tls-mode REQUIRED --tls-ca /path/to/ca.pem` | 🟢 REQUIRED+Custom CA | `user:pass@tcp(replica:3306)/db` | 🟢 Inherits Custom CA | Main: 🟢 REQUIRED+Custom<br>Replica: 🟢 REQUIRED+Custom | Corporate PKI |
| `--tls-mode PREFERRED --tls-ca /path/to/ca.pem` | 🟡 PREFERRED+Custom CA | `user:pass@tcp(replica:3306)/db` | 🟡 Inherits Custom CA | Main: 🟡 PREFERRED+Custom<br>Replica: 🟡 PREFERRED+Custom | Conditional with custom certs |
| `--tls-mode VERIFY_CA --tls-ca /path/to/ca.pem` | 🟢 VERIFY_CA+Custom CA | `user:pass@tcp(replica:3306)/db?tls=preferred` | � PREFERRED | Main: 🟢 VERIFY_CA+Custom<br>Replica: � PREFERRED | Secure main, conditional replica |
| **RDS Auto-Detection Scenarios** |
| *(no --tls-mode)* + RDS host | 🟡 PREFERRED+RDS certs | `user:pass@tcp(replica.rds.amazonaws.com:3306)/db` | 🟡 Inherits RDS certs | Main: 🟡 PREFERRED+RDS<br>Replica: 🟡 PREFERRED+RDS | AWS RDS deployment |
| `--tls-mode PREFERRED` + RDS host | 🟡 PREFERRED+RDS | `user:pass@tcp(replica:3306)/db?tls=false` | 🔴 NO TLS | Main: 🟡 PREFERRED+RDS<br>Replica: 🔴 NO TLS | RDS main, local replica |