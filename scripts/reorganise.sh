#!/bin/bash
# reorganise.sh — HateWatch project structure setup
# Run from inside /Users/hadi/git/hatewatch
# Usage: bash scripts/reorganise.sh

set -e  # stop on any error

PROJECT_DIR="/Users/hadi/git/hatewatch"
cd "$PROJECT_DIR"

echo "🗂  Setting up HateWatch project structure..."

# ---------------------------------------------------------------------------
# 1. Create new directories
# ---------------------------------------------------------------------------
echo "→ Creating directories..."
mkdir -p pipeline
mkdir -p review_ui/templates
mkdir -p review_ui/static
mkdir -p db/migrations
mkdir -p config
mkdir -p logs
mkdir -p reports
mkdir -p tests
mkdir -p scripts

# ---------------------------------------------------------------------------
# 2. Move existing files to correct locations
# ---------------------------------------------------------------------------
echo "→ Moving files..."

# Pipeline scripts
[ -f jotform_sync.py ]  && mv jotform_sync.py  pipeline/jotform_sync.py  && echo "  ✓ jotform_sync.py  → pipeline/"
[ -f validate.py ]      && mv validate.py       pipeline/validate.py      && echo "  ✓ validate.py      → pipeline/"

# Database
[ -f schema.sql ]       && mv schema.sql        db/schema.sql             && echo "  ✓ schema.sql       → db/"

# Tests
[ -f test_jotform.py ]  && mv test_jotform.py   tests/test_jotform.py     && echo "  ✓ test_jotform.py  → tests/"

# Logs
[ -f sync.log ]         && mv sync.log          logs/sync.log             && echo "  ✓ sync.log         → logs/"

# ---------------------------------------------------------------------------
# 3. Remove the ~/git/logs folder we created by mistake
# ---------------------------------------------------------------------------
if [ -d "$HOME/git/logs" ]; then
    rmdir "$HOME/git/logs" 2>/dev/null && echo "  ✓ removed ~/git/logs (empty folder)"
fi

# ---------------------------------------------------------------------------
# 4. Update .gitignore
# ---------------------------------------------------------------------------
echo "→ Updating .gitignore..."
cat >> .gitignore << 'EOF'

# Logs and reports
logs/
reports/
*.log

# Python
__pycache__/
*.pyc
*.pyo
.Python

# Temp files
*.tmp
*.bak
EOF
echo "  ✓ .gitignore updated"

# ---------------------------------------------------------------------------
# 5. Create pipeline.sh — runs full pipeline in one command
# ---------------------------------------------------------------------------
echo "→ Creating scripts/pipeline.sh..."
cat > scripts/pipeline.sh << 'EOF'
#!/bin/bash
# pipeline.sh — run full HateWatch pipeline
# Usage: bash scripts/pipeline.sh [--full]

set -e
cd "$(dirname "$0")/.."

SYNC_FLAG=""
if [ "$1" == "--full" ]; then
    SYNC_FLAG="--full"
    echo "🔄 Running FULL sync..."
else
    echo "🔄 Running incremental sync..."
fi

echo ""
echo "Step 1: JotForm sync"
uv run python pipeline/jotform_sync.py $SYNC_FLAG

echo ""
echo "Step 2: Data validation (dry run)"
uv run python pipeline/validate.py --report reports/latest_report.json

echo ""
echo "✅ Pipeline complete. Check reports/latest_report.json for details."
EOF
chmod +x scripts/pipeline.sh
echo "  ✓ scripts/pipeline.sh created"

# ---------------------------------------------------------------------------
# 6. Update log paths in pipeline scripts
# ---------------------------------------------------------------------------
echo "→ Updating log file paths in pipeline scripts..."

# Update jotform_sync.py log path
if [ -f pipeline/jotform_sync.py ]; then
    sed -i.bak "s|logging.FileHandler('sync.log')|logging.FileHandler('logs/sync.log')|g" pipeline/jotform_sync.py
    rm -f pipeline/jotform_sync.py.bak
    echo "  ✓ jotform_sync.py log path updated"
fi

# ---------------------------------------------------------------------------
# 7. Show final structure
# ---------------------------------------------------------------------------
echo ""
echo "✅ Done! Final structure:"
echo ""
find . -not -path './.git/*' -not -path './.venv/*' -not -path './__pycache__/*' \
    | sort \
    | sed 's|[^/]*/|  |g'

echo ""
echo "Next steps:"
echo "  1. Run: bash scripts/pipeline.sh        # test the pipeline"
echo "  2. Run: git add -A && git commit -m 'Reorganise project structure'"
