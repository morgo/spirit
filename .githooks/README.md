# Git Hooks for Spirit

This directory contains Git hooks that help maintain code quality by running automated checks before pushes.

## Setup

```bash
make setup-hooks
# Or run directly: ./scripts/setup-git-hooks.sh
```

This configures Git to use hooks from `.githooks` instead of `.git/hooks`.

**Requirements:** Docker must be installed and running.

## What Happens When You Push

When you run `git push`, the `pre-push` hook automatically:
1. Runs `golangci-lint` in a Docker container
2. Checks your code against all configured linters
3. **Blocks the push** if any errors are found
4. Does not interfere with local commits — your workflow stays fast

#### ✅ Successful push
```
$ git push
Running golangci-lint in Docker before push...
✅ Linting passed! Pushing...
```

#### ❌ Blocked push
```
$ git push
Running golangci-lint in Docker before push...

pkg/example/file.go:42:2: Error: use errors.Is() instead of == (errorlint)

❌ Linting failed! Please fix the errors before pushing.
```

## Fixing Lint Errors

```bash
# View all errors
make lint

# Auto-fix what can be fixed automatically
make lint-fix

# Then commit and push again
git add . && git commit -m "fix: address lint errors" && git push
```

## Bypassing Hooks

```bash
# Skip for a single push (not recommended)
git push --no-verify

# Disable hooks entirely
git config --unset core.hooksPath

# Re-enable later
make setup-hooks
```

## Customizing Hooks

All hooks are regular shell scripts. Edit them directly in this directory:
1. Modify `pre-push`
2. Ensure it stays executable: `chmod +x .githooks/pre-push`

## Platform Independence

Hooks use Docker to ensure consistent behavior across macOS, Linux, and Windows (Git Bash/WSL) — same linting environment regardless of local OS or Go version.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `docker: command not found` | Install Docker Desktop |
| `Permission denied: .githooks/pre-push` | Run `chmod +x .githooks/pre-push` |
| Hooks not running | Run `git config core.hooksPath` — should output `.githooks`. If empty, run `make setup-hooks` |
| Docker image pull fails | Check internet access and that Docker is running. First run downloads ~200MB |
| Exit code 1 with no visible errors | Run `make lint` manually to verify |

## CI/CD Integration

The pre-push hook complements (not replaces) CI/CD checks:
- **Pre-push hook**: Catches errors before code reaches the remote
- **CI pipeline**: Final enforcement before merge

Both use the same Docker image and configuration for consistency.
