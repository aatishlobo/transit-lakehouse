#!/usr/bin/env bash
#
# Build the course submission ZIP.
#
# The handout is specific: one ZIP containing exactly ONE top-level folder. A
# ZIP that explodes files into the grader's working directory is the classic way
# to lose marks before anyone reads the code.
#
# Deliberately assembles from a clean list rather than zipping the working
# directory, so nothing incidental (venv, caches, the 1.3 GB live archive, .env,
# logs) can ride along. An exclusion list would fail open; an inclusion list
# fails closed.
#
# Usage:
#   scripts/build_submission.sh alobo
#   scripts/build_submission.sh alobo bkarimi
#
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"

if [ $# -lt 1 ]; then
    echo "usage: $0 <usf_username> [teammate_username]" >&2
    exit 2
fi

if [ $# -eq 2 ]; then
    NAME="final_project_${1}_${2}"
else
    NAME="final_project_${1}"
fi

STAGE="$(mktemp -d)/$NAME"
mkdir -p "$STAGE"

echo "staging -> $STAGE"

# --- required top-level documents ---------------------------------------
for f in README.md DATA_SOURCE.md AI_USAGE.md requirements.txt .env.example \
         report.pdf Makefile docker-compose.yml; do
    if [ ! -e "$f" ]; then
        echo "MISSING required file: $f" >&2
        exit 1
    fi
    cp "$f" "$STAGE/"
done

# --- code ----------------------------------------------------------------
for d in ingest streaming ml profiling evaluation tests scripts; do
    rsync -a --exclude='__pycache__' --exclude='*.pyc' \
             --exclude='artifacts/*.joblib' \
             --exclude='data/features.csv' \
             "$d" "$STAGE/"
done

# Model artifact is wanted (the course asks for a model artifact) but excluded
# above so it can be copied deliberately rather than by accident.
mkdir -p "$STAGE/ml/artifacts"
cp ml/artifacts/*.joblib ml/artifacts/*.json "$STAGE/ml/artifacts/"

# --- data, outputs, evidence ---------------------------------------------
mkdir -p "$STAGE/data"
rsync -a data/replay_sample "$STAGE/data/"

cp -r outputs "$STAGE/"
cp -r docs "$STAGE/"
cp CLAUDE.md "$STAGE/" 2>/dev/null || true

# --- verify no secrets or bulk ------------------------------------------
echo
echo "=== safety checks ==="
if find "$STAGE" -name '.env' -o -name '*.log' | grep -q .; then
    echo "FAIL: secrets or logs present" >&2
    find "$STAGE" -name '.env' -o -name '*.log' >&2
    exit 1
fi
echo "  ok: no .env or logs"

if [ -n "${API_511_KEY:-}" ] && grep -rqF "$API_511_KEY" "$STAGE" 2>/dev/null; then
    echo "FAIL: API key found in staged content" >&2
    exit 1
fi
echo "  ok: no API key in staged content"

if [ -d "$STAGE/data/raw" ]; then
    echo "FAIL: live archive staged" >&2
    exit 1
fi
echo "  ok: live archive excluded"

if find "$STAGE" -name 'features.csv' | grep -q .; then
    echo "FAIL: 131MB feature table staged" >&2
    exit 1
fi
echo "  ok: full feature table excluded"

# --- package -------------------------------------------------------------
OUT="$ROOT/${NAME}.zip"
rm -f "$OUT"
( cd "$(dirname "$STAGE")" && zip -qr "$OUT" "$NAME" -x '*.DS_Store' )

echo
echo "=== result ==="
echo "  $OUT"
echo "  $(du -h "$OUT" | cut -f1)"
echo "  $(unzip -l "$OUT" | tail -1 | awk '{print $2}') files"
echo
echo "top-level entries inside the zip (must be exactly one):"
unzip -l "$OUT" | awk 'NR>3 {print $4}' | cut -d/ -f1 | sort -u | grep -v '^$'
