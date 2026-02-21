#!/bin/bash

set -e

echo "🔍 Checking Python version..."

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed or not in PATH"
    exit 1
fi

# Get Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
REQUIRED_VERSION="3.10"

# Compare versions
if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 10) else 1)"; then
    echo "❌ Error: Python 3.10+ is required, but you have Python $PYTHON_VERSION"
    echo "   Please install Python 3.10 or newer and try again"
    exit 1
fi

echo "✅ Found Python $PYTHON_VERSION"

# Create virtual environment
echo "📦 Creating virtual environment at ./venv..."
python3 -m venv venv

# Activate and install dependencies
echo "📥 Installing dependencies from requirements.txt..."
source venv/bin/activate
pip install --upgrade pip > /dev/null 2>&1
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Start Redis:        docker compose up -d"
echo "   2. Activate venv:      source venv/bin/activate"
echo "   3. Run the tracker:    python status_tracker.py"
echo ""
