#!/bin/bash
set -e

echo "Setting up pre-commit hooks..."
echo ""

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed or not in PATH"
    echo "   Please install Python 3.12+ first"
    exit 1
fi

echo "✓ Found Python: $(python3 --version)"

# Try different methods to install pre-commit
echo ""
echo "Installing pre-commit..."

# Method 1: Try pip3
if command -v pip3 &> /dev/null; then
    echo "  Using pip3..."
    pip3 install pre-commit
# Method 2: Try python3 -m pip
elif python3 -m pip --version &> /dev/null; then
    echo "  Using python3 -m pip..."
    python3 -m pip install pre-commit
else
    echo "❌ Error: Could not find pip. Please install pip first:"
    echo "   python3 -m ensurepip --upgrade"
    exit 1
fi

# Verify installation
if ! command -v pre-commit &> /dev/null; then
    echo ""
    echo "⚠️  Warning: pre-commit command not found in PATH"
    echo "   Trying to use python3 -m pre_commit instead..."

    # Install git hooks using python module
    python3 -m pre_commit install

    echo ""
    echo "✓ Pre-commit installed via Python module"
    echo "   To run hooks manually, use: python3 -m pre_commit run --all-files"
else
    echo "✓ Pre-commit installed successfully"

    # Install git hooks
    echo ""
    echo "Installing pre-commit git hooks..."
    pre-commit install

    echo ""
    echo "✓ Pre-commit hooks installed successfully!"
    echo ""
    echo "You can now test the hooks with:"
    echo "  pre-commit run --all-files"
fi

echo ""
echo "Hooks will now run automatically on 'git commit'"
echo ""
echo "To verify installation, try:"
echo "  git commit --allow-empty -m 'test pre-commit hooks'"
