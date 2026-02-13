#!/bin/bash

set -e

cd "$(dirname "$0")/.."

PROJECT_DIR="$(pwd)"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/maj_meteo_prec.log"
LOCK_FILE="/tmp/maj_meteo_prec.lock"

mkdir -p "$LOG_DIR"

if [ -f "$LOCK_FILE" ]; then
    echo "MAJ_METEO_PREC déjà en cours." >> "$LOG_FILE"
    exit 1
fi

touch "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

echo "----------------------------------------" >> "$LOG_FILE"
echo "Début MAJ_METEO_PREC : $(date)" >> "$LOG_FILE"

docker compose run --rm app MAJ_METEO_PREC >> "$LOG_FILE" 2>&1

echo "Fin MAJ_METEO_PREC : $(date)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

