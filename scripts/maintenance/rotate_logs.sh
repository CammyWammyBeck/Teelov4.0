#!/usr/bin/env bash
# rotate_logs.sh — Log rotation and pipeline artifact cleanup for Teelo
# - cron_hourly.log: rotate weekly, keep 4 weeks compressed
# - logs/pipeline/: delete run dirs older than 7 days (via Docker since dirs are root-owned)
set -euo pipefail

TEELO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_FILE="$TEELO_DIR/logs/cron_hourly.log"
PIPELINE_DIR="$TEELO_DIR/logs/pipeline"
ROTATE_DIR="$TEELO_DIR/logs/rotated"

mkdir -p "$ROTATE_DIR"

# --- Rotate cron_hourly.log if older than 7 days or too large ---
if [ -f "$LOG_FILE" ]; then
    MTIME=$(stat -c %Y "$LOG_FILE" 2>/dev/null || stat -f %m "$LOG_FILE" 2>/dev/null)
    NOW=$(date +%s)
    AGE_DAYS=$(( (NOW - MTIME) / 86400 ))
    SIZE_BYTES=$(stat -c %s "$LOG_FILE" 2>/dev/null || stat -f %z "$LOG_FILE" 2>/dev/null)
    MAX_BYTES=$((50 * 1024 * 1024))

    if [ "$AGE_DAYS" -ge 7 ] || [ "$SIZE_BYTES" -ge "$MAX_BYTES" ]; then
        STAMP=$(date +%Y%m%d-%H%M%S)
        ROTATED="$ROTATE_DIR/cron_hourly.$STAMP.log"
        cp "$LOG_FILE" "$ROTATED"
        gzip -f "$ROTATED"
        truncate -s 0 "$LOG_FILE"
        echo "[rotate_logs] Rotated cron_hourly.log → rotated/cron_hourly.$STAMP.log.gz"
    else
        echo "[rotate_logs] cron_hourly.log is ${AGE_DAYS}d old and ${SIZE_BYTES} bytes — no rotation needed"
    fi
fi

# --- Delete old pipeline artifact dirs (keep last 7 days) ---
# Dirs are root-owned (created by Docker), so we use a Docker container to remove them
if [ -d "$PIPELINE_DIR" ]; then
    # Collect dirs older than 7 days
    OLD_DIRS=()
    while IFS= read -r -d '' dir; do
        OLD_DIRS+=("$(basename "$dir")")
    done < <(find "$PIPELINE_DIR" -maxdepth 1 -mindepth 1 -type d -mtime +7 -print0)

    if [ "${#OLD_DIRS[@]}" -gt 0 ]; then
        echo "[rotate_logs] Deleting ${#OLD_DIRS[@]} pipeline artifact dirs older than 7 days..."
        # Build a list of paths relative to /app/logs/pipeline inside the container
        DELETE_ARGS=()
        for d in "${OLD_DIRS[@]}"; do
            DELETE_ARGS+=("/app/logs/pipeline/$d")
        done
        cd "$TEELO_DIR" && docker compose run --rm --no-deps --entrypoint="" teelo-update \
            bash -c "rm -rf $(printf '%q ' "${DELETE_ARGS[@]}")"
        echo "[rotate_logs] Deleted ${#OLD_DIRS[@]} pipeline artifact dirs"
    else
        echo "[rotate_logs] No pipeline artifact dirs older than 7 days"
    fi
fi

# --- Prune old rotated logs (keep 4 weeks) ---
PRUNED=0
while IFS= read -r -d '' f; do
    rm -f "$f"
    PRUNED=$((PRUNED + 1))
done < <(find "$ROTATE_DIR" -maxdepth 1 -name "cron_hourly.*.log.gz" -mtime +28 -print0)
if [ "$PRUNED" -gt 0 ]; then
    echo "[rotate_logs] Pruned $PRUNED old rotated log archive(s)"
fi

echo "[rotate_logs] Done."
