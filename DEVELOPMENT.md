## Minimum Requirements

Spirit requires go 1.25 or higher. MySQL version 8.0 and higher is required for performing schema changes.

## Running tests

The tests require a MySQL server to run. If you have MySQL installed locally, you can provide it as an environment variable:

```bash
MYSQL_DSN="root:mypassword@tcp(127.0.0.1:3306)/test" go test -v ./...
```
If the `MYSQL_DSN` is not specified, it currently defaults to `spirit:spirit@tcp(127.0.0.1:3306)/test`. This may change in the future.


### Running tests with docker on specific MySQL version
```bash
cd compose/
docker compose down --volumes && docker compose up -f compose.yml -f 8.0.28.yml 
docker compose up mysql test --abort-on-container-exit
```

## Running linter

### Using Docker (recommended - platform independent)

```bash
# Run lint checks
make lint

# Or run directly with Docker
docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:latest golangci-lint run --timeout=5m

# Run with auto-fix
make lint-fix
```

### Using local golangci-lint

```bash
golangci-lint run
```

## Git Hooks

To enable automatic linting before pushes:

```bash
# One-time setup
make setup-hooks

# Or run the script directly
./scripts/setup-git-hooks.sh
```

This configures a pre-push hook that runs golangci-lint in Docker, catching errors before they reach the remote repository. Commits remain fast and unblocked. See `.githooks/README.md` for more details.
