#!/bin/bash
# Setup script to configure Git hooks for the Spirit project
# This enables automatic linting checks before pushes

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_DIR="$REPO_ROOT/.githooks"
GIT_HOOKS_DIR="$REPO_ROOT/.git/hooks"

echo "Setting up Git hooks for Spirit..."

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "⚠️  Warning: Docker is not installed or not in PATH"
    echo "   Git hooks will be configured but won't work until Docker is available"
    echo ""
fi

# Configure Git to use our custom hooks directory
echo "Configuring Git to use .githooks directory..."
git config core.hooksPath "$HOOKS_DIR"

# Make all hook scripts executable
echo "Making hook scripts executable..."
chmod +x "$HOOKS_DIR"/*

echo ""
echo "✅ Git hooks setup complete!"
echo ""
echo "The following hooks are now active:"
echo "  - pre-push: Runs golangci-lint in Docker before each push"
echo ""
echo "To bypass hooks for a specific push (not recommended):"
echo "  git push --no-verify"
echo ""
echo "To disable hooks entirely:"
echo "  git config --unset core.hooksPath"
echo ""
