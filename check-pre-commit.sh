#!/bin/bash

echo "=== Pre-commit Diagnostic Check ==="
echo ""

# Check Python
echo "1. Checking Python..."
if command -v python3 &> /dev/null; then
    echo "   ✓ Python found: $(python3 --version)"
else
    echo "   ❌ Python 3 not found"
    exit 1
fi

# Check pip
echo ""
echo "2. Checking pip..."
if command -v pip3 &> /dev/null; then
    echo "   ✓ pip3 found: $(pip3 --version | head -1)"
    PIP_CMD="pip3"
elif python3 -m pip --version &> /dev/null 2>&1; then
    echo "   ✓ python3 -m pip available"
    PIP_CMD="python3 -m pip"
else
    echo "   ❌ pip not found"
    exit 1
fi

# Check pre-commit installation
echo ""
echo "3. Checking pre-commit installation..."
if command -v pre-commit &> /dev/null; then
    echo "   ✓ pre-commit command found: $(pre-commit --version)"
    PRE_COMMIT_CMD="pre-commit"
elif python3 -m pre_commit --version &> /dev/null 2>&1; then
    echo "   ✓ pre-commit available via python3 -m pre_commit"
    PRE_COMMIT_CMD="python3 -m pre_commit"
else
    echo "   ❌ pre-commit not installed"
    echo ""
    echo "   To install, run:"
    echo "     $PIP_CMD install pre-commit"
    exit 1
fi

# Check git hooks
echo ""
echo "4. Checking git hooks..."
if [ -f .git/hooks/pre-commit ]; then
    echo "   ✓ Pre-commit hook file exists"
    if [ -x .git/hooks/pre-commit ]; then
        echo "   ✓ Hook file is executable"
    else
        echo "   ⚠️  Hook file is not executable (run: chmod +x .git/hooks/pre-commit)"
    fi
else
    echo "   ❌ Pre-commit hook file NOT found"
    echo ""
    echo "   To install hooks, run:"
    echo "     $PRE_COMMIT_CMD install"
    exit 1
fi

# Check config file
echo ""
echo "5. Checking configuration..."
if [ -f .pre-commit-config.yaml ]; then
    echo "   ✓ .pre-commit-config.yaml exists"
else
    echo "   ❌ .pre-commit-config.yaml not found"
    exit 1
fi

echo ""
echo "=== All checks passed! ==="
echo ""
echo "To test hooks, run:"
echo "  $PRE_COMMIT_CMD run --all-files"
echo ""
echo "Hooks should run automatically on 'git commit'"
