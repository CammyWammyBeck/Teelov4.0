#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db import PipelineRun, PipelineStageRun, get_engine, get_session
from teelo.tasks import (
    StageContext,
    StageDefinition,
    StageRegistry,
    StageResult,
    advisory_lock_key,
    postgres_advisory_lock,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _append_jsonl(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    _ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


def _run_update_current_events_stage(ctx: StageContext) -> StageResult:
    started_at = _utc_now()
    stage_dir = ctx.artifacts_dir / ctx.stage_name
    _ensure_dir(stage_dir)

    workers = int(ctx.options.get("workers", 3))
    tours = ctx.options.get("tours")
    headless = bool(ctx.options.get("headless", False))
    clear_queue = bool(ctx.options.get("clear_queue", False))
    fast = bool(ctx.options.get("fast", True))

    metrics_json = stage_dir / "metrics.json"
    status_jsonl = stage_dir / "status.jsonl"

    cmd = [
        sys.executable,
        "scripts/update_current_events.py",
        "--workers",
        str(workers),
        "--quiet-worker-logs",
        "--metrics-json",
        str(metrics_json),
        "--status-jsonl",
        str(status_jsonl),
    ]
    if tours:
        cmd.extend(["--tours", tours])
    if fast:
        cmd.append("--fast")
    else:
        cmd.append("--no-fast")
    if headless:
        # update_current_events uses settings unless --headed is set.
        # We keep this flag for consistency in orchestrator options.
        pass
    if clear_queue:
        cmd.append("--clear-queue")

    print(f"[Stage {ctx.stage_name}] Running: {' '.join(cmd)}")
    started_perf = perf_counter()
    completed = subprocess.run(cmd, check=False)
    elapsed = perf_counter() - started_perf

    metrics_payload: dict[str, Any] | None = None
    if metrics_json.exists():
        try:
            metrics_payload = json.loads(metrics_json.read_text(encoding="utf-8"))
        except Exception:
            metrics_payload = None

    metrics: dict[str, Any] = {
        "exit_code": completed.returncode,
        "elapsed_s": elapsed,
        "command": cmd,
        "metrics_json": str(metrics_json),
        "status_jsonl": str(status_jsonl),
    }
    if metrics_payload is not None:
        aggregate = metrics_payload.get("aggregate") or {}
        metrics["aggregate"] = {
            "tasks_processed": aggregate.get("tasks_processed"),
            "tasks_completed": aggregate.get("tasks_completed"),
            "tasks_failed": aggregate.get("tasks_failed"),
            "timings": aggregate.get("timings"),
        }

    ended_at = _utc_now()
    if completed.returncode != 0:
        return StageResult(
            stage_name=ctx.stage_name,
            status="failed",
            started_at=started_at,
            ended_at=ended_at,
            metrics=metrics,
            error=f"update_current_events exited with code {completed.returncode}",
        )

    return StageResult(
        stage_name=ctx.stage_name,
        status="success",
        started_at=started_at,
        ended_at=ended_at,
        metrics=metrics,
    )


def _run_script_stage(script_path: str):
    def _runner(ctx: StageContext) -> StageResult:
        started_at = _utc_now()
        stage_dir = ctx.artifacts_dir / ctx.stage_name
        _ensure_dir(stage_dir)
        metrics_json = stage_dir / "metrics.json"
        status_jsonl = stage_dir / "status.jsonl"

        cmd = [
            sys.executable,
            script_path,
            "--metrics-json",
            str(metrics_json),
            "--status-jsonl",
            str(status_jsonl),
        ]
        print(f"[Stage {ctx.stage_name}] Running: {' '.join(cmd)}")
        started_perf = perf_counter()
        completed = subprocess.run(cmd, check=False)
        elapsed = perf_counter() - started_perf
        metrics: dict[str, Any] = {
            "exit_code": completed.returncode,
            "elapsed_s": elapsed,
            "command": cmd,
            "metrics_json": str(metrics_json),
            "status_jsonl": str(status_jsonl),
        }

        stage_status = "success"
        stage_error: str | None = None
        if completed.returncode != 0:
            stage_status = "failed"
            stage_error = f"{script_path} exited with code {completed.returncode}"

        script_metrics: dict[str, Any] | None = None
        if metrics_json.exists():
            try:
                script_metrics = json.loads(metrics_json.read_text(encoding="utf-8"))
            except Exception:
                script_metrics = None

        if script_metrics is not None:
            metrics["script_metrics"] = script_metrics
            payload_status = script_metrics.get("status")
            if payload_status in {"success", "failed", "partial", "skipped"}:
                stage_status = payload_status
                if payload_status == "failed" and stage_error is None:
                    stage_error = str(script_metrics.get("error") or "stage script reported failure")

        ended_at = _utc_now()
        return StageResult(
            stage_name=ctx.stage_name,
            status=stage_status,  # type: ignore[arg-type]
            started_at=started_at,
            ended_at=ended_at,
            metrics=metrics,
            error=stage_error,
        )

    return _runner


def _save_run_started(run_id: str, started_at: datetime) -> None:
    with get_session() as session:
        session.add(
            PipelineRun(
                run_id=run_id,
                started_at=started_at,
                status="running",
            )
        )


def _save_stage_result(run_id: str, result: StageResult) -> None:
    with get_session() as session:
        session.add(
            PipelineStageRun(
                run_id=run_id,
                stage_name=result.stage_name,
                started_at=result.started_at,
                ended_at=result.ended_at,
                status=result.status,
                metrics_json=result.metrics,
                error_text=result.error,
            )
        )


def _save_run_finished(
    run_id: str,
    ended_at: datetime,
    status: str,
    summary: dict[str, Any],
) -> None:
    with get_session() as session:
        run = session.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
        if run is None:
            raise RuntimeError(f"PipelineRun not found for run_id={run_id}")
        run.ended_at = ended_at
        run.status = status
        run.summary_json = summary


async def _execute_stage(stage: StageDefinition, ctx: StageContext) -> StageResult:
    outcome = stage.runner(ctx)
    if inspect.isawaitable(outcome):
        return await outcome
    return outcome


def _build_registry() -> StageRegistry:
    registry = StageRegistry()
    registry.register(
        StageDefinition(
            name="current_events_ingest",
            runner=_run_update_current_events_stage,
            description="Scrape + ingest current tournaments (no downstream jobs).",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="elo_incremental",
            runner=_run_script_stage("scripts/update_elo_incremental.py"),
            description="Apply incremental ELO updates for newly terminal matches.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="player_enrichment_incremental",
            runner=_run_script_stage("scripts/update_players_incremental.py"),
            description="Enrich players requiring profile metadata.",
            enabled_by_default=False,
        )
    )
    return registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run hourly pipeline stages in sequence.")
    parser.add_argument(
        "--stages",
        default=None,
        help="Comma-separated stage list. Default: registry defaults.",
    )
    parser.add_argument(
        "--skip-stages",
        default="",
        help="Comma-separated stage names to skip.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=True,
        help="Stop on first failed stage (default).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running remaining stages after failures.",
    )
    parser.add_argument(
        "--artifacts-dir",
        default="logs/pipeline",
        help="Base directory for run artifacts.",
    )
    parser.add_argument(
        "--metrics-json",
        default=None,
        help="Write orchestrator summary JSON to this path.",
    )
    parser.add_argument(
        "--status-jsonl",
        default=None,
        help="Append run/stage events as JSONL to this path.",
    )
    parser.add_argument(
        "--lock-name",
        default="teelo_hourly_pipeline",
        help="Advisory lock namespace.",
    )
    parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=5.0,
        help="Advisory lock acquisition timeout.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Workers for current_events_ingest stage.",
    )
    parser.add_argument(
        "--tours",
        default=None,
        help="Optional tours override passed to update_current_events.",
    )
    parser.add_argument(
        "--clear-queue",
        action="store_true",
        help="Clear queue before current_events_ingest stage.",
    )
    parser.add_argument(
        "--no-fast",
        action="store_true",
        help="Disable fast mode in current_events_ingest.",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    started_at = _utc_now()
    run_id = started_at.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    artifacts_root = Path(args.artifacts_dir) / run_id
    _ensure_dir(artifacts_root)
    events_path = Path(args.status_jsonl) if args.status_jsonl else None

    registry = _build_registry()
    include = [s.strip() for s in args.stages.split(",")] if args.stages else None
    skip = {s.strip() for s in args.skip_stages.split(",") if s.strip()}
    stages = registry.resolve(include=include, skip=skip)

    run_summary: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "stages": [],
        "status": "running",
    }

    _append_jsonl(
        events_path,
        {
            "event": "pipeline_started",
            "run_id": run_id,
            "timestamp": _utc_now().isoformat(),
            "stages": [s.name for s in stages],
        },
    )

    _save_run_started(run_id, started_at)

    lock = advisory_lock_key(args.lock_name)
    engine = get_engine()

    try:
        with postgres_advisory_lock(
            engine,
            key=lock,
            timeout_seconds=args.lock_timeout_seconds,
        ):
            for stage in stages:
                stage_started = _utc_now()
                ctx = StageContext(
                    run_id=run_id,
                    stage_name=stage.name,
                    started_at=stage_started,
                    artifacts_dir=artifacts_root,
                    options={
                        "workers": args.workers,
                        "tours": args.tours,
                        "clear_queue": args.clear_queue,
                        "fast": not args.no_fast,
                    },
                )
                result = await _execute_stage(stage, ctx)
                _save_stage_result(run_id, result)
                run_summary["stages"].append(result.to_dict())

                _append_jsonl(
                    events_path,
                    {
                        "event": "stage_finished",
                        "run_id": run_id,
                        "timestamp": _utc_now().isoformat(),
                        **result.to_dict(),
                    },
                )

                if result.status == "failed" and not args.continue_on_error:
                    break
    except TimeoutError as exc:
        ended_at = _utc_now()
        run_summary["ended_at"] = ended_at.isoformat()
        run_summary["status"] = "failed"
        run_summary["error"] = str(exc)
        _save_run_finished(run_id, ended_at, "failed", run_summary)
        _append_jsonl(
            events_path,
            {
                "event": "pipeline_failed",
                "run_id": run_id,
                "timestamp": _utc_now().isoformat(),
                "error": str(exc),
            },
        )
        if args.metrics_json:
            _write_json(Path(args.metrics_json), run_summary)
        print(f"Pipeline failed to acquire lock: {exc}")
        return 2

    ended_at = _utc_now()
    has_failed = any(stage["status"] == "failed" for stage in run_summary["stages"])
    final_status = "failed" if has_failed else "success"
    run_summary["ended_at"] = ended_at.isoformat()
    run_summary["status"] = final_status
    run_summary["duration_s"] = (ended_at - started_at).total_seconds()

    _save_run_finished(run_id, ended_at, final_status, run_summary)

    _append_jsonl(
        events_path,
        {
            "event": "pipeline_finished",
            "run_id": run_id,
            "timestamp": _utc_now().isoformat(),
            "status": final_status,
            "duration_s": run_summary["duration_s"],
        },
    )

    if args.metrics_json:
        _write_json(Path(args.metrics_json), run_summary)

    print(f"Pipeline {run_id} finished with status={final_status}")
    return 1 if final_status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
