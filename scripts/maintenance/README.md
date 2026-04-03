# scripts/maintenance

Small ops/maintenance helpers for the Teelo pipeline.

## pipeline_health_check.py
Generates a Discord-friendly health report for the pipeline over the last N hours (default: 12h).

**What it checks (high level):**
- Pipeline run success/failure summary + average duration
- Recent activity counts (matches created/completed, predictions made, etc.)
- Scrape queue failures (new + accumulated) grouped by rough error type
- Stage-level failures in the window
- Queue depth snapshot
- Log/artifact disk usage (best-effort)

**Run:**
```bash
python scripts/maintenance/pipeline_health_check.py --hours 12
```

Notes:
- Assumes it can import the app via `src/` (the script injects `src` onto `sys.path`).
- Uses the configured DB connection from `teelo.db.get_engine()`.

## rotate_logs.sh
Rotates and prunes local logs/artifacts.

**Behaviour:**
- Rotates `logs/cron_hourly.log` when it is >= 7 days old
  - Keeps 4 weeks of compressed archives in `logs/rotated/`
- Deletes pipeline artifact dirs under `logs/pipeline/` older than 7 days
  - Uses `docker compose run ...` because those dirs are typically root-owned (created inside containers)

**Run:**
```bash
bash scripts/maintenance/rotate_logs.sh
```

## Suggested cron
Adjust paths/user as needed.

Example (daily at 03:15):
```cron
15 3 * * * cd /path/to/Teelov4.0 && bash scripts/maintenance/rotate_logs.sh >> logs/cron_hourly.log 2>&1
```

Example (twice daily health report to stdout; wire into Discord however you prefer):
```cron
0 6,18 * * * cd /path/to/Teelov4.0 && python scripts/maintenance/pipeline_health_check.py --hours 12
```
