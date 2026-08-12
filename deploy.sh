#!/bin/bash
set -e
echo "=== Starting Railway Deploy ==="
echo "Date: $(date)"
echo "Commit: $(git rev-parse --short HEAD)"
echo "Commit message: $(git log -1 --pretty=%B)"

# Push to GitHub first
git push origin main 2>&1 || echo "Push failed (may already be up to date)"

# Deploy via Railway
echo ""
echo "=== Running railway up ==="
railway up 2>&1

echo ""
echo "=== Done ==="
