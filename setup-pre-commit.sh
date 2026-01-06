#!/bin/bash
set -e

echo "Setting up pre-commit hooks..."

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed or not in PATH"
    exit 1
fi

# Check if pip3 is available
if ! command -v pip3 &> /dev/null; then
    echo "Error: pip3 is not installed or not in PATH"
    exit 1
fi

echo "Installing pre-commit..."
pip3 install pre-commit

echo "Installing pre-commit git hooks..."
pre-commit install

echo "✓ Pre-commit hooks installed successfully!"
echo ""
echo "You can now test the hooks with:"
echo "  pre-commit run --all-files"
echo ""
echo "Hooks will now run automatically on 'git commit'"

