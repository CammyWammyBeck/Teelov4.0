#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import shutil
import sys
from time import perf_counter
from typing import Any

from sqlalchemy import and_, or_, update

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy.dialects.postgresql import insert

from teelo.db import Match, PlayerEloState, Tournament, TournamentEdition, get_session
from teelo.elo.boost import calculate_k_boost
from teelo.elo.calculator import calculate_fast
from teelo.elo.constants import get_level_code
from teelo.elo.decay import apply_inactivity_decay
from teelo.elo.margin import calculate_margin_multiplier
from teelo.elo.params_store import get_active_elo_params
from teelo.elo.pipeline import date_from_temporal_order
from teelo.tasks import DBCheckpointStore


TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")


@dataclass
class PlayerState:
    player_id: int
    rating: float = 1500.0
    match_count: int = 0
    last_temporal_order: int | None = None
    last_match_date: date | None = None
    career_peak: float = 1500.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _append_jsonl(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


def _to_decimal(value: float) -> Decimal:
    return Decimal(str(round(value, 2)))


def _days_since(last_date: date | None, current_date: date | None) -> int | None:
    if last_date is None or current_date is None:
        return None
    return (current_date - last_date).days


def _preload_player_states(session, player_ids: set[int]) -> dict[int, PlayerState]:
    if not player_ids:
        return {}
    existing = (
        session.query(PlayerEloState)
        .filter(PlayerEloState.player_id.in_(player_ids))
        .all()
    )
    state_by_player: dict[int, PlayerState] = {
        int(row.player_id): PlayerState(
            player_id=int(row.player_id),
            rating=float(row.rating),
            match_count=int(row.match_count),
            last_temporal_order=int(row.last_temporal_order) if row.last_temporal_order is not None else None,
            last_match_date=row.last_match_date,
            career_peak=float(row.career_peak),
        )
        for row in existing
    }
    for player_id in player_ids:
        if player_id not in state_by_player:
            state_by_player[player_id] = PlayerState(player_id=player_id)
    return state_by_player


def _refresh_pending_pre_snapshots(
    session,
    *,
    params_version: str,
    touched_player_ids: set[int] | None,
    refresh_all_pending: bool,
    batch_size: int,
) -> int:
    """Populate/refresh pre-match ELO snapshots for pending matches."""
    updated_total = 0
    last_id = 0
    touched_ids = {int(player_id) for player_id in (touched_player_ids or set())}

    while True:
        query = session.query(Match).filter(
            Match.id > last_id,
            Match.status.in_(("upcoming", "scheduled")),
            Match.winner_id.is_(None),
        )

        if not refresh_all_pending:
            pending_filters = [
                Match.elo_pre_player_a.is_(None),
                Match.elo_pre_player_b.is_(None),
            ]
            if touched_ids:
                pending_filters.extend(
                    [
                        Match.player_a_id.in_(touched_ids),
                        Match.player_b_id.in_(touched_ids),
                    ]
                )
            query = query.filter(or_(*pending_filters))

        matches = query.order_by(Match.id.asc()).limit(batch_size).all()
        if not matches:
            break

        player_ids = {int(match.player_a_id) for match in matches}
        player_ids.update(int(match.player_b_id) for match in matches)
        state_by_player = _preload_player_states(session, player_ids)

        updates: list[dict[str, Any]] = []
        for match in matches:
            state_a = state_by_player[int(match.player_a_id)]
            state_b = state_by_player[int(match.player_b_id)]
            updates.append(
                {
                    "id": int(match.id),
                    "elo_pre_player_a": _to_decimal(state_a.rating),
                    "elo_pre_player_b": _to_decimal(state_b.rating),
                    "elo_params_version": params_version,
                }
            )

        if updates:
            session.execute(update(Match), updates)
            updated_total += len(updates)
        last_id = int(matches[-1].id)

    return updated_total


class LiveEloProgress:
    """Two-line live terminal progress for ELO updates, inspired by LiveWorkerDashboard.

    When stdout is a TTY, overwrites two lines in place using ANSI escape codes.
    When piped or redirected, prints one permanent line per batch completion instead.
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled and sys.stdout.isatty()
        self._initialized = False
        self._batch_line = ""
        self._summary_line = ""

    def _fit(self, line: str) -> str:
        """Truncate line to terminal width with '...' suffix if needed."""
        width = max(40, shutil.get_terminal_size((120, 24)).columns - 1)
        if len(line) > width:
            return line[: max(0, width - 3)] + "..."
        return line

    def update(self, batch_line: str, summary_line: str) -> None:
        """Overwrite the two live lines in place (TTY mode only)."""
        self._batch_line = self._fit(batch_line)
        self._summary_line = self._fit(summary_line)
        if not self.enabled:
            return
        if not self._initialized:
            # First call: just print the two lines so we have something to overwrite
            print(self._batch_line)
            print(self._summary_line)
            self._initialized = True
            return
        # Move cursor up 2 lines, clear each, and rewrite
        sys.stdout.write("\x1b[2A")
        sys.stdout.write(f"\x1b[2K\r{self._batch_line}\n")
        sys.stdout.write(f"\x1b[2K\r{self._summary_line}\n")
        sys.stdout.flush()

    def batch_done(self, line: str) -> None:
        """Print a permanent line for a completed batch (non-TTY fallback)."""
        if not self.enabled:
            print(self._fit(line))

    def finish(self) -> None:
        """Print a blank line after the last update to separate from final summary."""
        if self.enabled and self._initialized:
            print()


def _format_summary_line(payload: dict[str, Any], elapsed: float) -> str:
    """Format the running totals line for live progress display."""
    rate = payload["processed"] / elapsed if elapsed > 0 else 0
    return (
        f"Processed: {payload['processed']} | Updated: {payload['updated']} | "
        f"Skipped: {payload['skipped']} | Errors: {payload['errors']} | "
        f"{elapsed:.1f}s ({rate:.0f}/s)"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply incremental ELO updates for terminal matches.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Maximum matches per DB batch.")
    parser.add_argument("--max-matches", type=int, default=0, help="Optional cap on processed matches (0 = no cap).")
    parser.add_argument(
        "--checkpoint-key",
        default="elo_incremental",
        help="Checkpoint key in pipeline_checkpoints.",
    )
    parser.add_argument(
        "--resume",
        dest="resume",
        action="store_true",
        default=True,
        help="Resume from checkpoint cursor (default).",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Ignore checkpoint cursor and scan from beginning.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Ignore checkpoint cursor even when --resume is enabled.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Full replay mode: clear inline ELO artifacts and process all terminal matches from the beginning.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updates/checkpoints.")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Emit per-batch progress every N processed matches.",
    )
    parser.add_argument(
        "--checkpoint-every-batches",
        type=int,
        default=5,
        help="Persist checkpoint every N batches (default: 5).",
    )
    parser.add_argument("--metrics-json", default=None, help="Write stage metrics JSON")
    parser.add_argument("--status-jsonl", default=None, help="Append status events JSONL")
    parser.add_argument(
        "--refresh-pending-all",
        action="store_true",
        help="Refresh pre-match ELO for all pending matches (upcoming/scheduled), regardless of touched players.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    status_path = Path(args.status_jsonl) if args.status_jsonl else None
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    payload: dict[str, Any] = {
        "stage": "elo_incremental",
        "mode": "rebuild" if args.rebuild else "incremental",
        "started_at": started_at.isoformat(),
        "batch_size": args.batch_size,
        "max_matches": args.max_matches,
        "checkpoint_key": args.checkpoint_key,
        "resume": args.resume,
        "full_scan": args.full_scan,
        "dry_run": args.dry_run,
        "rebuild": args.rebuild,
        "refresh_pending_all": args.refresh_pending_all,
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "error_examples": [],
        "batches": 0,
        "pending_pre_updated": 0,
        "checkpoint_in": None,
        "checkpoint_out": None,
    }

    _append_jsonl(
        status_path,
        {
            "event": "elo_incremental_started",
            "timestamp": _utc_now_iso(),
            "batch_size": args.batch_size,
            "max_matches": args.max_matches,
            "resume": args.resume,
            "full_scan": args.full_scan,
            "dry_run": args.dry_run,
            "rebuild": args.rebuild,
            "refresh_pending_all": args.refresh_pending_all,
            "checkpoint_every_batches": args.checkpoint_every_batches,
        },
    )

    with get_session() as session:
        checkpoint_store = DBCheckpointStore(session)
        cursor: dict[str, int] | None = None
        checkpoint_written = False
        if (not args.rebuild) and args.resume and not args.full_scan:
            checkpoint = checkpoint_store.get(args.checkpoint_key)
            if checkpoint:
                checkpoint_cursor = checkpoint.get("cursor") or {}
                temporal = checkpoint_cursor.get("last_temporal_order")
                match_id = checkpoint_cursor.get("last_match_id")
                if isinstance(temporal, int) and isinstance(match_id, int):
                    cursor = {
                        "last_temporal_order": temporal,
                        "last_match_id": match_id,
                    }
                    payload["checkpoint_in"] = cursor
                    _append_jsonl(
                        status_path,
                        {
                            "event": "elo_incremental_checkpoint_loaded",
                            "timestamp": _utc_now_iso(),
                            "checkpoint_key": args.checkpoint_key,
                            "cursor": cursor,
                        },
                    )

        params, params_version = get_active_elo_params(session)
        processed_cap = args.max_matches if args.max_matches > 0 else None
        run_perf_start = perf_counter()

        if args.rebuild and not args.dry_run:
            session.query(PlayerEloState).delete()
            session.query(Match).update(
                {
                    Match.elo_pre_player_a: None,
                    Match.elo_pre_player_b: None,
                    Match.elo_post_player_a: None,
                    Match.elo_post_player_b: None,
                    Match.elo_params_version: None,
                    Match.elo_processed_at: None,
                    Match.elo_needs_recompute: False,
                },
                synchronize_session=False,
            )
            session.flush()

        # Print startup header
        resume_info = ""
        if cursor:
            resume_info = f" | Resume: temporal={cursor['last_temporal_order']} match={cursor['last_match_id']}"
        print(f"ELO INCREMENTAL UPDATE")
        mode_label = "REBUILD" if args.rebuild else "INCREMENTAL"
        print(f"Mode: {mode_label} | Params: {params_version} | Batch: {args.batch_size}{resume_info}")
        print("\u2500" * 60)

        progress = LiveEloProgress()
        run_touched_player_ids: set[int] = set()

        while True:
            if processed_cap is not None and payload["processed"] >= processed_cap:
                break

            query = (
                session.query(Match, Tournament.level, Tournament.tour)
                .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
                .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
            )
            if args.rebuild:
                query = query.filter(
                    Match.status.in_(TERMINAL_STATUSES),
                    Match.winner_id.isnot(None),
                    Match.temporal_order.isnot(None),
                )
            else:
                query = query.filter(
                    Match.status.in_(TERMINAL_STATUSES),
                    Match.winner_id.isnot(None),
                    Match.temporal_order.isnot(None),
                    or_(
                        Match.elo_post_player_a.is_(None),
                        Match.elo_post_player_b.is_(None),
                        Match.elo_needs_recompute.is_(True),
                    ),
                )

            if cursor is not None:
                query = query.filter(
                    or_(
                        Match.temporal_order > cursor["last_temporal_order"],
                        and_(
                            Match.temporal_order == cursor["last_temporal_order"],
                            Match.id > cursor["last_match_id"],
                        ),
                    )
                )

            query = query.order_by(Match.temporal_order.asc(), Match.id.asc())
            query_limit = args.batch_size
            if processed_cap is not None:
                query_limit = min(query_limit, processed_cap - payload["processed"])
            _append_jsonl(
                status_path,
                {
                    "event": "elo_incremental_query_started",
                    "timestamp": _utc_now_iso(),
                    "batch_index": payload["batches"] + 1,
                    "query_limit": query_limit,
                    "cursor": cursor,
                    "totals": {
                        "processed": payload["processed"],
                        "updated": payload["updated"],
                        "skipped": payload["skipped"],
                        "errors": payload["errors"],
                    },
                },
            )
            batch_num = payload["batches"] + 1
            overall_elapsed = perf_counter() - run_perf_start
            progress.update(
                f"Batch {batch_num} | Querying...",
                _format_summary_line(payload, overall_elapsed),
            )
            batch_query_start = perf_counter()
            rows = query.limit(query_limit).all()
            query_elapsed = perf_counter() - batch_query_start
            _append_jsonl(
                status_path,
                {
                    "event": "elo_incremental_query_finished",
                    "timestamp": _utc_now_iso(),
                    "batch_index": payload["batches"] + 1,
                    "query_limit": query_limit,
                    "rows_fetched": len(rows),
                    "query_elapsed_s": round(query_elapsed, 4),
                    "cursor": cursor,
                },
            )
            if not rows:
                break

            payload["batches"] += 1
            batch_index = payload["batches"]
            batch_processed = 0
            batch_updated = 0
            batch_skipped = 0
            batch_errors = 0
            preload_elapsed = 0.0
            batch_process_start = perf_counter()
            should_checkpoint = (
                (not args.rebuild)
                and
                cursor is not None
                and args.checkpoint_every_batches > 0
                and (batch_index % args.checkpoint_every_batches == 0)
            )
            player_ids = {int(match.player_a_id) for match, _, _ in rows}
            player_ids.update(int(match.player_b_id) for match, _, _ in rows)
            _append_jsonl(
                status_path,
                {
                    "event": "elo_incremental_state_preload_started",
                    "timestamp": _utc_now_iso(),
                    "batch_index": batch_index,
                    "batch_size": len(rows),
                    "player_ids": len(player_ids),
                },
            )
            overall_elapsed = perf_counter() - run_perf_start
            progress.update(
                f"Batch {batch_index} | Preloading {len(player_ids)} player states...",
                _format_summary_line(payload, overall_elapsed),
            )
            preload_start = perf_counter()
            state_by_player = _preload_player_states(session, player_ids)
            preload_elapsed = perf_counter() - preload_start
            _append_jsonl(
                status_path,
                {
                    "event": "elo_incremental_state_preload_finished",
                    "timestamp": _utc_now_iso(),
                    "batch_index": batch_index,
                    "player_ids": len(player_ids),
                    "preload_elapsed_s": round(preload_elapsed, 4),
                },
            )
            match_updates: list[dict[str, Any]] = []
            recompute_match_ids: list[int] = []  # IDs needing elo_needs_recompute=True
            touched_players: set[int] = set()
            for row_index, row in enumerate(rows, start=1):
                match, tournament_level, tournament_tour = row
                payload["processed"] += 1
                batch_processed += 1
                cursor = {
                    "last_temporal_order": int(match.temporal_order),
                    "last_match_id": int(match.id),
                }

                if args.dry_run:
                    continue

                try:
                    level_code = get_level_code(tournament_level, tournament_tour)
                    state_a = state_by_player[int(match.player_a_id)]
                    state_b = state_by_player[int(match.player_b_id)]

                    if (
                        (state_a.last_temporal_order is not None and match.temporal_order < state_a.last_temporal_order)
                        or (state_b.last_temporal_order is not None and match.temporal_order < state_b.last_temporal_order)
                    ):
                        recompute_match_ids.append(int(match.id))
                        payload["skipped"] += 1
                        batch_skipped += 1
                        continue

                    match_date = match.match_date or date_from_temporal_order(match.temporal_order)
                    # Always continue from the current rolling player state.
                    # Using persisted match-level pre snapshots here can reintroduce stale values.
                    before_a = float(state_a.rating)
                    before_b = float(state_b.rating)

                    if state_a.last_match_date is not None and match_date is not None:
                        before_a = apply_inactivity_decay(
                            before_a,
                            (match_date - state_a.last_match_date).days,
                            decay_rate=params.decay_rate,
                            decay_start_days=params.decay_start_days,
                        )
                    if state_b.last_match_date is not None and match_date is not None:
                        before_b = apply_inactivity_decay(
                            before_b,
                            (match_date - state_b.last_match_date).days,
                            decay_rate=params.decay_rate,
                            decay_start_days=params.decay_start_days,
                        )

                    days_a = _days_since(state_a.last_match_date, match_date)
                    days_b = _days_since(state_b.last_match_date, match_date)
                    boost_a = calculate_k_boost(
                        state_a.match_count,
                        float(days_a) if days_a is not None else None,
                        new_threshold=params.new_threshold,
                        new_boost=params.new_boost,
                        returning_days=params.returning_days,
                        returning_boost=params.returning_boost,
                    )
                    boost_b = calculate_k_boost(
                        state_b.match_count,
                        float(days_b) if days_b is not None else None,
                        new_threshold=params.new_threshold,
                        new_boost=params.new_boost,
                        returning_days=params.returning_days,
                        returning_boost=params.returning_boost,
                    )

                    margin_mult = 1.0
                    if match.score_structured:
                        winner = "A" if match.winner_id == match.player_a_id else "B"
                        margin_mult = float(
                            calculate_margin_multiplier(
                                match.score_structured,
                                winner,
                                margin_base=params.margin_base,
                                margin_scale=params.margin_scale,
                            ).multiplier
                        )
                    else:
                        winner = "A" if match.winner_id == match.player_a_id else "B"

                    base_k = params.get_k(level_code)
                    s = params.get_s(level_code)
                    new_a, new_b, _ = calculate_fast(
                        before_a,
                        before_b,
                        winner,
                        base_k * margin_mult * boost_a,
                        base_k * margin_mult * boost_b,
                        s,
                    )
                    new_a = round(new_a, 2)
                    new_b = round(new_b, 2)

                    state_a.rating = new_a
                    state_b.rating = new_b
                    state_a.match_count += 1
                    state_b.match_count += 1
                    state_a.last_temporal_order = int(match.temporal_order)
                    state_b.last_temporal_order = int(match.temporal_order)
                    if match_date is not None:
                        state_a.last_match_date = match_date
                        state_b.last_match_date = match_date
                    state_a.career_peak = max(state_a.career_peak, new_a)
                    state_b.career_peak = max(state_b.career_peak, new_b)
                    touched_players.add(state_a.player_id)
                    touched_players.add(state_b.player_id)

                    match_updates.append(
                        {
                            "id": int(match.id),
                            "elo_pre_player_a": _to_decimal(before_a),
                            "elo_pre_player_b": _to_decimal(before_b),
                            "elo_post_player_a": _to_decimal(new_a),
                            "elo_post_player_b": _to_decimal(new_b),
                            "elo_params_version": params_version,
                            "elo_processed_at": _utc_now(),
                            "elo_needs_recompute": False,
                        }
                    )
                    payload["updated"] += 1
                    batch_updated += 1
                except Exception as exc:
                    payload["errors"] += 1
                    batch_errors += 1
                    if len(payload["error_examples"]) < 10:
                        payload["error_examples"].append(
                            {
                                "match_id": int(match.id),
                                "external_id": match.external_id,
                                "error": str(exc),
                            }
                        )

                if args.progress_every > 0 and (
                    (row_index % args.progress_every == 0) or row_index == len(rows)
                ):
                    elapsed = perf_counter() - batch_process_start
                    rate = row_index / elapsed if elapsed > 0 else 0
                    overall_elapsed = perf_counter() - run_perf_start
                    progress.update(
                        f"Batch {batch_index} | Processing {row_index}/{len(rows)} ({rate:,.0f}/s)",
                        _format_summary_line(payload, overall_elapsed),
                    )
                    _append_jsonl(
                        status_path,
                        {
                            "event": "elo_incremental_batch_progress",
                            "timestamp": _utc_now_iso(),
                            "batch_index": batch_index,
                            "row_index": row_index,
                            "batch_size": len(rows),
                            "batch_processed": batch_processed,
                            "batch_updated": batch_updated,
                            "batch_skipped": batch_skipped,
                            "batch_errors": batch_errors,
                            "process_elapsed_s": round(elapsed, 4),
                            "process_rate_per_s": round((row_index / elapsed), 3) if elapsed > 0 else 0.0,
                        },
                    )
            process_elapsed = perf_counter() - batch_process_start

            commit_elapsed = 0.0
            checkpoint_elapsed = 0.0
            if args.dry_run:
                session.rollback()
            else:
                overall_elapsed = perf_counter() - run_perf_start
                progress.update(
                    f"Batch {batch_index} | Writing {len(match_updates)} updates + {len(touched_players)} states...",
                    _format_summary_line(payload, overall_elapsed),
                )
                write_start = perf_counter()
                # Recompute flags: single UPDATE ... WHERE id IN (...) — 1 round-trip
                if recompute_match_ids:
                    session.execute(
                        update(Match)
                        .where(Match.id.in_(recompute_match_ids))
                        .values(elo_needs_recompute=True)
                    )
                # Full ELO updates: executemany with execute_batch (batched by engine)
                if match_updates:
                    session.execute(update(Match), match_updates)
                if touched_players:
                    run_touched_player_ids.update(touched_players)
                    now = _utc_now()
                    state_rows = []
                    for player_id in touched_players:
                        state = state_by_player[player_id]
                        state_rows.append(
                            {
                                "player_id": player_id,
                                "rating": _to_decimal(state.rating),
                                "match_count": state.match_count,
                                "last_temporal_order": state.last_temporal_order,
                                "last_match_date": state.last_match_date,
                                "career_peak": _to_decimal(state.career_peak),
                                "updated_at": now,
                            }
                        )
                    state_stmt = insert(PlayerEloState).values(state_rows)
                    state_stmt = state_stmt.on_conflict_do_update(
                        index_elements=[PlayerEloState.player_id],
                        set_={
                            "rating": state_stmt.excluded.rating,
                            "match_count": state_stmt.excluded.match_count,
                            "last_temporal_order": state_stmt.excluded.last_temporal_order,
                            "last_match_date": state_stmt.excluded.last_match_date,
                            "career_peak": state_stmt.excluded.career_peak,
                            "updated_at": state_stmt.excluded.updated_at,
                        },
                    )
                    session.execute(state_stmt)
                bulk_write_elapsed = perf_counter() - write_start
                _append_jsonl(
                    status_path,
                    {
                        "event": "elo_incremental_bulk_write_finished",
                        "timestamp": _utc_now_iso(),
                        "batch_index": batch_index,
                        "match_updates": len(match_updates),
                        "recompute_flags": len(recompute_match_ids),
                        "state_updates": len(touched_players),
                        "bulk_write_elapsed_s": round(bulk_write_elapsed, 4),
                    },
                )
                if should_checkpoint:
                    _append_jsonl(
                        status_path,
                        {
                            "event": "elo_incremental_checkpoint_write_started",
                            "timestamp": _utc_now_iso(),
                            "batch_index": batch_index,
                            "checkpoint_key": args.checkpoint_key,
                        },
                    )
                    checkpoint_start = perf_counter()
                    checkpoint_store.set(
                        args.checkpoint_key,
                        {
                            "cursor": cursor,
                            "updated_at": _utc_now_iso(),
                        },
                    )
                    checkpoint_elapsed = perf_counter() - checkpoint_start
                    _append_jsonl(
                        status_path,
                        {
                            "event": "elo_incremental_checkpoint_write_finished",
                            "timestamp": _utc_now_iso(),
                            "batch_index": batch_index,
                            "checkpoint_elapsed_s": round(checkpoint_elapsed, 4),
                            "cursor": cursor,
                        },
                    )
                    checkpoint_written = True
                _append_jsonl(
                    status_path,
                    {
                        "event": "elo_incremental_commit_started",
                        "timestamp": _utc_now_iso(),
                        "batch_index": batch_index,
                        "batch_size": len(rows),
                    },
                )
                overall_elapsed = perf_counter() - run_perf_start
                progress.update(
                    f"Batch {batch_index} | Committing...",
                    _format_summary_line(payload, overall_elapsed),
                )
                commit_start = perf_counter()
                session.commit()
                commit_elapsed = perf_counter() - commit_start
                _append_jsonl(
                    status_path,
                    {
                        "event": "elo_incremental_commit_finished",
                        "timestamp": _utc_now_iso(),
                        "batch_index": batch_index,
                        "commit_elapsed_s": round(commit_elapsed, 4),
                    },
                )
            batch_elapsed = query_elapsed + preload_elapsed + process_elapsed + commit_elapsed + checkpoint_elapsed
            overall_elapsed = perf_counter() - run_perf_start
            batch_rate = (batch_processed / batch_elapsed) if batch_elapsed > 0 else 0.0
            overall_rate = (payload["processed"] / overall_elapsed) if overall_elapsed > 0 else 0.0

            _append_jsonl(
                status_path,
                {
                    "event": "elo_incremental_batch",
                    "timestamp": _utc_now_iso(),
                    "batch_index": batch_index,
                    "batch_size": len(rows),
                    "batch_processed": batch_processed,
                    "batch_updated": batch_updated,
                    "batch_skipped": batch_skipped,
                    "batch_errors": batch_errors,
                    "totals": {
                        "processed": payload["processed"],
                        "updated": payload["updated"],
                        "skipped": payload["skipped"],
                        "errors": payload["errors"],
                    },
                    "cursor": cursor,
                    "timings_s": {
                        "query": round(query_elapsed, 4),
                        "preload_states": round(preload_elapsed, 4),
                        "process": round(process_elapsed, 4),
                        "commit": round(commit_elapsed, 4),
                        "checkpoint": round(checkpoint_elapsed, 4),
                        "batch_total": round(batch_elapsed, 4),
                        "run_elapsed": round(overall_elapsed, 4),
                    },
                    "rates_per_s": {
                        "batch": round(batch_rate, 3),
                        "overall": round(overall_rate, 3),
                    },
                    "dry_run": args.dry_run,
                },
            )
            # Non-TTY fallback: one permanent line per batch
            progress.batch_done(
                f"Batch {batch_index}: {batch_processed} matches "
                f"({batch_updated} updated, {batch_skipped} skipped, {batch_errors} errors) "
                f"in {batch_elapsed:.1f}s"
            )

        progress.finish()

        if not args.dry_run:
            refresh_all_pending = args.rebuild or args.refresh_pending_all or payload["processed"] == 0
            pending_pre_updated = _refresh_pending_pre_snapshots(
                session,
                params_version=params_version,
                touched_player_ids=run_touched_player_ids,
                refresh_all_pending=refresh_all_pending,
                batch_size=max(1000, args.batch_size),
            )
            payload["pending_pre_updated"] = pending_pre_updated
            session.commit()

        if cursor is not None and not args.rebuild:
            payload["checkpoint_out"] = cursor
            if not args.dry_run and not checkpoint_written:
                checkpoint_store.set(
                    args.checkpoint_key,
                    {
                        "cursor": cursor,
                        "updated_at": _utc_now_iso(),
                    },
                )
                session.commit()

    ended_at = datetime.now(timezone.utc).replace(tzinfo=None)
    payload["ended_at"] = ended_at.isoformat()
    payload["duration_s"] = (ended_at - started_at).total_seconds()
    payload["status"] = "success"

    _append_jsonl(
        status_path,
        {
            "event": "elo_incremental_finished",
            "timestamp": _utc_now_iso(),
            "status": payload["status"],
            "processed": payload["processed"],
            "updated": payload["updated"],
            "skipped": payload["skipped"],
            "errors": payload["errors"],
            "duration_s": payload["duration_s"],
        },
    )

    if args.metrics_json:
        _write_json(Path(args.metrics_json), payload)

    rate = payload["processed"] / payload["duration_s"] if payload["duration_s"] > 0 else 0
    print(
        "ELO incremental complete: "
        f"processed={payload['processed']} updated={payload['updated']} "
        f"pending_pre_updated={payload['pending_pre_updated']} "
        f"skipped={payload['skipped']} errors={payload['errors']} "
        f"in {payload['duration_s']:.1f}s ({rate:.0f}/s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
