#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import structlog

from teelo.db import PipelineRun, PipelineStageRun, ActivityLog, get_engine, get_session
from teelo.tasks import (
    StageContext,
    StageDefinition,
    StageRegistry,
    StageResult,
    advisory_lock_key,
    postgres_advisory_lock,
)

logger = structlog.get_logger(__name__)


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


def _run_script_stage(script_path: str, extra_args: list[str] | None = None):
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
        ]
        if extra_args:
            cmd.extend(extra_args)
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


def _run_feature_computation_stage(ctx: StageContext) -> StageResult:
    """Compute features for matches missing them."""
    started_at = _utc_now()
    try:
        from teelo.features import build_registry, default_preset_for_feature_set
        from teelo.features.engine import FeatureEngine
        from teelo.ml.versioning import latest_feature_set
        feature_set_name = latest_feature_set()
        registry = build_registry(default_preset_for_feature_set(feature_set_name))
        engine = FeatureEngine(registry, feature_set_name)
        engine.run()
        logger.info("stage.feature_computation_done")
        return StageResult(
            stage_name=ctx.stage_name, status="success",
            started_at=started_at, ended_at=_utc_now(),
        )
    except Exception as exc:
        logger.error("stage.feature_computation_failed", error=str(exc))
        return StageResult(
            stage_name=ctx.stage_name, status="failed",
            started_at=started_at, ended_at=_utc_now(), error=str(exc),
        )


def _run_predictions_stage(ctx: StageContext) -> StageResult:
    """Run batch predictions on upcoming matches."""
    started_at = _utc_now()
    try:
        from teelo.ml.predictor import BatchPredictor
        predictor = BatchPredictor()
        count = predictor.predict()
        logger.info("stage.predictions_done", count=count)
        return StageResult(
            stage_name=ctx.stage_name, status="success",
            started_at=started_at, ended_at=_utc_now(),
            metrics={"predicted_count": count},
        )
    except Exception as exc:
        logger.error("stage.predictions_failed", error=str(exc))
        return StageResult(
            stage_name=ctx.stage_name, status="failed",
            started_at=started_at, ended_at=_utc_now(), error=str(exc),
        )


def _run_forecast_node_sync_stage(ctx: StageContext) -> StageResult:
    """Promote scenario forecast nodes to actual nodes when real matches are now known."""
    started_at = _utc_now()
    lookback_days = int(ctx.options.get("forecast_sync_lookback_days", 21))
    lookahead_days = int(ctx.options.get("forecast_sync_lookahead_days", 14))
    statement_timeout_ms = int(ctx.options.get("forecast_sync_statement_timeout_ms", 120_000))

    try:
        from teelo.db import get_session
        from teelo.db.models import Tournament, TournamentEdition, TournamentForecastRun
        from teelo.services.tournament_forecast import is_forecast_eligible_tournament, sync_forecast_nodes
        from sqlalchemy import text

        today = date.today()
        window_start = today - timedelta(days=lookback_days)
        window_end = today + timedelta(days=lookahead_days)
        candidates: list[tuple[int, int, str]] = []

        with get_session() as session:
            rows = (
                session.query(TournamentForecastRun, TournamentEdition, Tournament)
                .join(TournamentEdition, TournamentEdition.id == TournamentForecastRun.tournament_edition_id)
                .join(Tournament, Tournament.id == TournamentEdition.tournament_id)
                .filter(
                    TournamentForecastRun.is_active.is_(True),
                    TournamentForecastRun.status == "ready",
                    TournamentEdition.start_date <= window_end,
                    TournamentEdition.end_date >= window_start,
                )
                .all()
            )
            for run, edition, tournament in rows:
                if not is_forecast_eligible_tournament(tournament):
                    continue
                label = f"{tournament.tour} {tournament.name} {edition.year}"
                candidates.append((int(run.id), int(edition.id), label))

        failures: list[dict[str, Any]] = []
        total_updated = 0
        for _run_id, edition_id, label in candidates:
            try:
                with get_session() as session:
                    session.execute(text("SET LOCAL statement_timeout = :timeout_ms"), {"timeout_ms": statement_timeout_ms})
                    updated = sync_forecast_nodes(session, edition_id=edition_id)
                total_updated += updated
                if updated:
                    logger.info("forecast_node_sync.updated", edition_id=edition_id, label=label, updated=updated)
            except Exception as exc:
                logger.warning("forecast_node_sync.edition_failed", edition_id=edition_id, label=label, error=str(exc))
                failures.append({"edition_id": edition_id, "label": label, "error": str(exc)[:500]})

        logger.info(
            "stage.forecast_node_sync_done",
            candidates=len(candidates),
            total_updated=total_updated,
            failures=len(failures),
        )
        status = "partial" if failures else "success"
        return StageResult(
            stage_name=ctx.stage_name,
            status=status,  # type: ignore[arg-type]
            started_at=started_at,
            ended_at=_utc_now(),
            metrics={
                "candidate_count": len(candidates),
                "nodes_promoted": total_updated,
                "failures": failures,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            },
            error=f"{len(failures)} forecast edition sync failure(s)" if failures else None,
        )
    except Exception as exc:
        logger.error("stage.forecast_node_sync_failed", error=str(exc))
        return StageResult(
            stage_name=ctx.stage_name,
            status="partial",
            started_at=started_at,
            ended_at=_utc_now(),
            metrics={"nodes_promoted": 0, "failures": [{"error": str(exc)[:500]}]},
            error=str(exc),
        )


def _run_metrics_snapshot_stage(ctx: StageContext) -> StageResult:
    """Compute and store prediction accuracy metrics."""
    started_at = _utc_now()
    try:
        from teelo.ml.metrics import compute_snapshot
        from teelo.ml.versioning import latest_model_path

        model_path = latest_model_path()
        # Predictions are now tagged with the artifact filename
        # (e.g. "prediction_v17.json"); metrics snapshots must match that to
        # group rows correctly.
        model_version = Path(model_path).name

        for source in ("live", "backfill", "all"):
            compute_snapshot(model_version=model_version, source_filter=source)
        logger.info("stage.metrics_snapshot_done")
        return StageResult(
            stage_name=ctx.stage_name, status="success",
            started_at=started_at, ended_at=_utc_now(),
        )
    except Exception as exc:
        logger.error("stage.metrics_snapshot_failed", error=str(exc))
        return StageResult(
            stage_name=ctx.stage_name, status="failed",
            started_at=started_at, ended_at=_utc_now(), error=str(exc),
        )


def _run_activity_log_stage(ctx: StageContext) -> StageResult:
    """Write customer-facing activity log entries based on what changed."""
    started_at = _utc_now()
    try:
        from teelo.db import get_session, Match, TournamentEdition, PipelineRun as PR

        with get_session() as session:
            run = session.query(PR).filter(PR.run_id == ctx.run_id).first()
            if not run:
                return StageResult(
                    stage_name=ctx.stage_name, status="skipped",
                    started_at=started_at, ended_at=_utc_now(),
                    error="Pipeline run not found",
                )
            since = run.started_at

            from sqlalchemy import func

            # match_created and tournament_created: query by created_at (reliable)
            match_created = (
                session.query(func.count(Match.id))
                .filter(Match.created_at >= since)
                .scalar() or 0
            )
            tournament_created = (
                session.query(func.count(TournamentEdition.id))
                .filter(TournamentEdition.created_at >= since)
                .scalar() or 0
            )

            # match_completed and prediction_made: read from sibling stage metrics
            # to avoid over-counting matches that were merely re-touched during scraping
            match_completed = 0
            prediction_made = 0
            stage_rows = (
                session.query(PipelineStageRun)
                .filter(
                    PipelineStageRun.run_id == ctx.run_id,
                    PipelineStageRun.stage_name.in_(["elo_incremental", "predictions"]),
                )
                .all()
            )
            for stage_row in stage_rows:
                m = stage_row.metrics_json or {}
                sm = m.get("script_metrics") or {}
                if stage_row.stage_name == "elo_incremental":
                    match_completed = sm.get("processed", 0)
                elif stage_row.stage_name == "predictions":
                    prediction_made = sm.get("predicted_count", 0)
                    if not prediction_made:
                        # in-process predictions stage stores count directly
                        prediction_made = m.get("predicted_count", 0)

            counts = {
                "match_created": match_created,
                "match_completed": match_completed,
                "prediction_made": prediction_made,
                "tournament_created": tournament_created,
            }

            messages = {
                "match_created": lambda n: f"{n} new match{'es' if n != 1 else ''} created",
                "match_completed": lambda n: f"{n} match{'es' if n != 1 else ''} completed",
                "prediction_made": lambda n: f"{n} prediction{'s' if n != 1 else ''} made",
                "tournament_created": lambda n: f"{n} tournament{'s' if n != 1 else ''} created",
            }

            logged = {}
            for event_type, count in counts.items():
                if count > 0:
                    entry = ActivityLog(
                        event_type=event_type,
                        count=count,
                        message=messages[event_type](count),
                        pipeline_run_id=ctx.run_id,
                    )
                    session.add(entry)
                    logged[event_type] = count

        logger.info("stage.activity_log_done", logged=logged)
        return StageResult(
            stage_name=ctx.stage_name, status="success",
            started_at=started_at, ended_at=_utc_now(),
            metrics={"logged": logged},
        )
    except Exception as exc:
        logger.error("stage.activity_log_failed", error=str(exc))
        return StageResult(
            stage_name=ctx.stage_name, status="failed",
            started_at=started_at, ended_at=_utc_now(), error=str(exc),
        )


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
            runner=_run_script_stage("scripts/update_elo.py"),
            description="Apply incremental ELO updates for newly terminal matches.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="player_enrichment_incremental",
            runner=_run_script_stage(
                "scripts/update_players_incremental.py",
                ["--max-players", "10"],
            ),
            description="Enrich players requiring profile metadata.",
            enabled_by_default=False,
        )
    )
    registry.register(
        StageDefinition(
            name="feature_computation",
            runner=_run_feature_computation_stage,
            description="Compute features for matches missing them.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="predictions",
            runner=_run_predictions_stage,
            description="Run ML predictions on upcoming/scheduled matches.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="forecast_node_sync",
            runner=_run_forecast_node_sync_stage,
            description="Promote scenario forecast nodes to actual nodes when real matches are now known.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="metrics_snapshot",
            runner=_run_metrics_snapshot_stage,
            description="Compute and store prediction accuracy metric snapshots.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="activity_log",
            runner=_run_activity_log_stage,
            description="Write customer-facing activity log entries.",
            enabled_by_default=True,
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
