#!/bin/bash

set -e

cd "$(dirname "$0")/.."

PROJECT_DIR="$(pwd)"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/maj_production.log"
LOCK_FILE="/tmp/maj_production.lock"

mkdir -p "$LOG_DIR"

if [ -f "$LOCK_FILE" ]; then
    echo "MAJ_PRODUCTION déjà en cours." >> "$LOG_FILE"
    exit 1
fi

touch "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

echo "----------------------------------------" >> "$LOG_FILE"
echo "Début MAJ_PRODUCTION : $(date)" >> "$LOG_FILE"

docker compose run --rm app MAJ_PROD >> "$LOG_FILE" 2>&1

echo "Fin MAJ_PRODUCTION : $(date)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

