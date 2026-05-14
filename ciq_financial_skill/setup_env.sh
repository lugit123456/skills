#!/bin/bash
# Capital IQ Skill - Environment Setup Script

set -e

SKILL_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "=== Capital IQ Skill Environment Check ==="
echo "Skill directory: $SKILL_DIR"
echo ""

# --- 1. Python Virtual Environment ---
echo "--- [1/3] Checking Python virtual environment ---"
VENV_DIR="$SKILL_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "  Creating virtual environment at $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
    echo "  ✓ Virtual environment created."
else
    echo "  ✓ Virtual environment exists: $VENV_DIR"
fi

# Activate and install deps
source "$VENV_DIR/bin/activate"
echo "  Installing/upgrading Python dependencies..."
pip install -q --upgrade pip
pip install -q -r "$SKILL_DIR/requirements.txt"
echo "  ✓ Dependencies installed."
echo ""

# --- 2. Install Playwright Browsers ---
echo "--- [2/3] Installing Playwright Chromium ---"
playwright install chromium
echo "  ✓ Playwright Chromium installed."
echo ""

# --- 3. Check .env configuration ---
echo "--- [3/3] Checking .env configuration ---"
ENV_FILE="$SKILL_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "  ✗ .env file NOT found at $ENV_FILE"
    echo "  Creating template .env file..."
    cat > "$ENV_FILE" <<'EOF'
# Capital IQ Credentials (required)
CIQ_USERNAME=
CIQ_PASSWORD=

# Optional settings
# CIQ_OUTPUT_DIR=./output
# CIQ_DEBUG=true  # Set to true to see the browser during login (useful for MFA)
EOF
    echo "  ✓ Template .env created. Please edit it with your credentials:"
    echo "    $ENV_FILE"
    echo ""
    exit 1
else
    # Check if credentials are filled
    source "$ENV_FILE" 2>/dev/null || true
    if [ -z "$CIQ_USERNAME" ] || [ -z "$CIQ_PASSWORD" ]; then
        echo "  ⚠ .env exists but CIQ_USERNAME or CIQ_PASSWORD is empty."
        echo "  Please edit: $ENV_FILE"
        exit 1
    else
        echo "  ✓ .env configured (email: $CIQ_USERNAME)"
    fi
fi
echo ""

echo "=== All checks passed! ==="
echo "Usage:"
echo "  source $VENV_DIR/bin/activate"
echo "  python $SKILL_DIR/search_and_download.py \"<company_name>\" [-n COUNT]"