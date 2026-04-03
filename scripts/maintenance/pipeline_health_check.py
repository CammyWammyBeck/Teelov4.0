#!/usr/bin/env python3
"""
pipeline_health_check.py — Twice-daily Teelo pipeline health report.

Queries the DB and logs to produce a Discord-formatted report covering
the last 12 hours of pipeline runs.

Usage:
    python scripts/maintenance/pipeline_health_check.py [--hours 12]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from teelo.db import get_engine
from sqlalchemy import text


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def fmt_size(path: Path) -> str:
    try:
        size = path.stat().st_size
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    except Exception:
        return "?"


def run_report(hours: int = 12) -> str:
    since = utc_now() - timedelta(hours=hours)
    since_naive = since.replace(tzinfo=None)
    engine = get_engine()

    lines = []
    lines.append(f"## 🩺 Teelo Pipeline Health — {utc_now().strftime('%Y-%m-%d %H:%M')} UTC")
    lines.append(f"*Covering last {hours}h*")
    lines.append("")

    with engine.connect() as conn:

        # ── 1. Pipeline run summary ──────────────────────────────────────
        runs = conn.execute(text("""
            SELECT run_id, status, started_at, ended_at,
                   EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at)) AS duration_s
            FROM pipeline_runs
            WHERE started_at >= :since
            ORDER BY started_at DESC
        """), {"since": since_naive}).fetchall()

        total = len(runs)
        succeeded = sum(1 for r in runs if r.status == "success")
        failed_runs = [r for r in runs if r.status == "failed"]
        stuck_runs = [r for r in runs if r.status == "running"]

        if total == 0:
            lines.append("⚠️ **No pipeline runs found in the last 12h — pipeline may be down!**")
        else:
            status_icon = "✅" if not failed_runs and not stuck_runs else "⚠️"
            lines.append(f"**Pipeline Runs:** {status_icon} {succeeded}/{total} succeeded")
            if failed_runs:
                for r in failed_runs:
                    lines.append(f"  ❌ `{r.run_id}` — failed")
            if stuck_runs:
                for r in stuck_runs:
                    age_min = int((utc_now().replace(tzinfo=None) - r.started_at).total_seconds() / 60)
                    lines.append(f"  ⏳ `{r.run_id}` — stuck as 'running' for {age_min}m (container may have died)")

        # Avg run duration (exclude stuck)
        completed_durations = [r.duration_s for r in runs if r.status in ("success", "failed") and r.duration_s]
        if completed_durations:
            avg_dur = sum(completed_durations) / len(completed_durations)
            lines.append(f"  Avg run duration: {avg_dur:.0f}s")

        lines.append("")

        # ── 2. Activity: matches + predictions ───────────────────────────
        activity = conn.execute(text("""
            SELECT event_type, SUM(count) as total
            FROM activity_log
            WHERE created_at >= :since
            GROUP BY event_type
            ORDER BY event_type
        """), {"since": since_naive}).fetchall()

        activity_map = {r.event_type: r.total for r in activity}
        matches_created = activity_map.get("match_created", 0)
        matches_completed = activity_map.get("match_completed", 0)
        predictions_made = activity_map.get("prediction_made", 0)
        tournaments_created = activity_map.get("tournament_created", 0)

        lines.append("**Activity (last 12h):**")
        lines.append(f"  Matches created: {matches_created}")
        lines.append(f"  Matches completed: {matches_completed}")
        lines.append(f"  Predictions made: {predictions_made}")
        if tournaments_created:
            lines.append(f"  Tournaments created: {tournaments_created}")
        lines.append("")

        # ── 3. Scrape queue failures ──────────────────────────────────────
        # New failures since the window
        new_failures = conn.execute(text("""
            SELECT
                task_params->>'tour_key' as tour,
                task_params->>'tournament_name' as name,
                attempts,
                CASE
                    WHEN last_error LIKE '%DeadlockDetected%' THEN 'DeadlockDetected'
                    WHEN last_error LIKE '%UniqueViolation%' THEN 'UniqueViolation'
                    WHEN last_error LIKE '%StringDataRightTruncation%' THEN 'StringDataTruncation'
                    WHEN last_error LIKE '%BrowserType.launch%'
                      OR last_error LIKE '%Connection closed%'
                      OR last_error LIKE '%Target page%' THEN 'BrowserCrash'
                    WHEN last_error LIKE '%TimeoutError%' THEN 'Timeout'
                    ELSE 'Other'
                END as error_type,
                COALESCE(completed_at, created_at) as updated_at
            FROM scrape_queue
            WHERE status = 'failed'
              AND COALESCE(completed_at, created_at) >= :since
            ORDER BY completed_at DESC NULLS LAST
        """), {"since": since_naive}).fetchall()

        # Total accumulated failures by error type/tour
        all_failures = conn.execute(text("""
            SELECT
                task_params->>'tour_key' as tour,
                CASE
                    WHEN last_error LIKE '%DeadlockDetected%' THEN 'DeadlockDetected'
                    WHEN last_error LIKE '%UniqueViolation%' THEN 'UniqueViolation'
                    WHEN last_error LIKE '%StringDataRightTruncation%' THEN 'StringDataTruncation'
                    WHEN last_error LIKE '%BrowserType.launch%'
                      OR last_error LIKE '%Connection closed%'
                      OR last_error LIKE '%Target page%' THEN 'BrowserCrash'
                    WHEN last_error LIKE '%TimeoutError%' THEN 'Timeout'
                    ELSE 'Other'
                END as error_type,
                COUNT(*) as count
            FROM scrape_queue
            WHERE status = 'failed'
            GROUP BY tour, error_type
            ORDER BY count DESC
        """)).fetchall()

        lines.append("**Scrape Queue Failures:**")

        if new_failures:
            lines.append(f"  🆕 New failures in last 12h: {len(new_failures)}")
            # Group by error type
            by_type: dict[str, list] = {}
            for f in new_failures:
                by_type.setdefault(f.error_type, []).append(f)
            for etype, items in sorted(by_type.items()):
                lines.append(f"    {etype} ({len(items)}): " +
                              ", ".join(f"{i.tour} {i.name}" for i in items[:5]) +
                              (" +" + str(len(items) - 5) + " more" if len(items) > 5 else ""))
        else:
            lines.append("  ✅ No new failures in last 12h")

        # Always show total accumulated backlog
        if all_failures:
            lines.append(f"  Total accumulated failures in queue:")
            for f in all_failures:
                # Flag known recurring issues
                recurring_note = ""
                if f.tour in ("ITF_WOMEN", "ITF_MEN") and f.error_type == "BrowserCrash":
                    recurring_note = " *(known ITF browser issue)*"
                lines.append(f"    {f.tour} / {f.error_type}: {f.count}{recurring_note}")

        lines.append("")

        # ── 4. Stage-level failures in this window ───────────────────────
        stage_failures = conn.execute(text("""
            SELECT psr.run_id, psr.stage_name, psr.status, psr.error_text,
                   pr.started_at
            FROM pipeline_stage_runs psr
            JOIN pipeline_runs pr ON pr.run_id = psr.run_id
            WHERE pr.started_at >= :since
              AND psr.status = 'failed'
            ORDER BY pr.started_at DESC
        """), {"since": since_naive}).fetchall()

        if stage_failures:
            lines.append("**Stage Failures (last 12h):**")
            for sf in stage_failures:
                lines.append(f"  ❌ `{sf.run_id}` / {sf.stage_name}: {(sf.error_text or '')[:100]}")
            lines.append("")

        # ── 5. Queue depth snapshot ───────────────────────────────────────
        queue_stats = conn.execute(text("""
            SELECT status, COUNT(*) as count
            FROM scrape_queue
            GROUP BY status
        """)).fetchall()
        qmap = {r.status: r.count for r in queue_stats}

        lines.append("**Queue Snapshot:**")
        lines.append(f"  Pending: {qmap.get('pending', 0)}")
        lines.append(f"  In-progress: {qmap.get('in_progress', 0)}")
        lines.append(f"  Completed (total): {qmap.get('completed', 0)}")
        lines.append(f"  Failed (total): {qmap.get('failed', 0)}")
        if (qmap.get('in_progress', 0) or 0) > 20:
            lines.append(f"  ⚠️ High in-progress count may indicate stuck tasks")
        lines.append("")

    # ── 6. Log file sizes ────────────────────────────────────────────────
    teelo_dir = Path(__file__).parent.parent.parent
    log_file = teelo_dir / "logs" / "cron_hourly.log"
    pipeline_dir = teelo_dir / "logs" / "pipeline"

    log_size = fmt_size(log_file)
    pipeline_count = len(list(pipeline_dir.iterdir())) if pipeline_dir.exists() else 0

    lines.append("**Log Storage:**")
    lines.append(f"  cron_hourly.log: {log_size}")
    lines.append(f"  Pipeline artifact dirs: {pipeline_count}")
    if log_file.exists() and log_file.stat().st_size > 50 * 1024 * 1024:
        lines.append("  ⚠️ cron_hourly.log > 50MB — rotation due")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=12)
    args = parser.parse_args()
    print(run_report(hours=args.hours))


if __name__ == "__main__":
    main()
