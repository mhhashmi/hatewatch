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
