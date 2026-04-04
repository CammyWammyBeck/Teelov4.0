#!/usr/bin/env python3
from __future__ import annotations

"""Cron-friendly tournament forecast runner.

Intended usage: run once per day (e.g. midnight) after the current-events
scrape/update pipeline has refreshed draws.

What it does:
- Finds *eligible* main-tour (ATP/WTA) tournament editions in a date window.
- For each edition:
    - checks whether the draw is forecast-ready
    - skips if an active usable forecast already exists
    - otherwise builds a forecast run

Examples:
    python scripts/run_tournament_forecasts.py --dry-run
    python scripts/run_tournament_forecasts.py --lookback-days 2 --lookahead-days 21
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from time import perf_counter

# Add src to path (match other scripts)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy.orm import joinedload

from teelo.db import get_session
from teelo.db.models import Tournament, TournamentEdition
from teelo.services.tournament_forecast import (
    build_forecast_run,
    get_active_run,
    is_draw_forecast_ready,
    is_forecast_eligible_tournament,
)


_ALLOWED_LEVELS_ATP = {"Grand Slam", "Masters 1000", "ATP 500", "ATP 250", "ATP Finals"}
_ALLOWED_LEVELS_WTA = {"Grand Slam", "WTA 1000", "WTA 500", "WTA 250", "WTA Finals"}


@dataclass(frozen=True)
class EditionCandidate:
    """Session-free representation of a candidate edition."""

    edition_id: int


def _fmt_edition(edition: TournamentEdition) -> str:
    t = edition.tournament
    name = ((t.name if t else None) or (t.tournament_code if t else None) or "?").strip()
    tour = (t.tour if t else "?")
    level = (t.level if t else "?")
    code = (t.tournament_code if t else "?")
    return f"{tour} {name} {edition.year} (edition_id={edition.id}, code={code}, level={level})"


def _load_candidates(*, today: date, lookback_days: int, lookahead_days: int) -> list[EditionCandidate]:
    window_start = today - timedelta(days=max(0, lookback_days))
    window_end = today + timedelta(days=max(0, lookahead_days))

    allowed_levels = _ALLOWED_LEVELS_ATP | _ALLOWED_LEVELS_WTA

    with get_session() as session:
        rows = (
            session.query(TournamentEdition.id)
            .join(Tournament, Tournament.id == TournamentEdition.tournament_id)
            .filter(
                Tournament.tour.in_(["ATP", "WTA"]),
                Tournament.level.in_(sorted(allowed_levels)),
                TournamentEdition.year == today.year,
                TournamentEdition.start_date.isnot(None),
                TournamentEdition.end_date.isnot(None),
                TournamentEdition.end_date >= window_start,
                TournamentEdition.start_date <= window_end,
            )
            .order_by(TournamentEdition.start_date.asc(), TournamentEdition.id.asc())
            .all()
        )

        # Apply canonical eligibility predicate (belt-and-suspenders).
        edition_ids: list[int] = [int(r[0]) for r in rows if r and r[0] is not None]
        if not edition_ids:
            return []

        editions = (
            session.query(TournamentEdition)
            .options(joinedload(TournamentEdition.tournament))
            .filter(TournamentEdition.id.in_(edition_ids))
            .all()
        )

        eligible_ids: list[int] = []
        for e in editions:
            if e.tournament and is_forecast_eligible_tournament(e.tournament):
                eligible_ids.append(int(e.id))

        # Keep stable ordering by the original ids ordering.
        eligible_set = set(eligible_ids)
        return [EditionCandidate(edition_id=eid) for eid in edition_ids if eid in eligible_set]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build tournament forecasts (cron-friendly)")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="Include editions whose end_date is up to N days in the past (default: 3)",
    )
    parser.add_argument(
        "--lookahead-days",
        type=int,
        default=14,
        help="Include editions whose start_date is up to N days in the future (default: 14)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild even if an active ready forecast exists (rare)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not build; only print what would be done",
    )
    args = parser.parse_args()

    started = perf_counter()
    today = date.today()

    print("=" * 60)
    print("TOURNAMENT FORECAST RUNNER")
    print("=" * 60)
    print(f"Date: {today.isoformat()}")
    print(f"Window: lookback_days={args.lookback_days}, lookahead_days={args.lookahead_days}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'BUILD'}{' (force)' if args.force else ''}")

    candidates = _load_candidates(today=today, lookback_days=args.lookback_days, lookahead_days=args.lookahead_days)
    print(f"Candidates found: {len(candidates)}")

    # Counters
    checked = 0
    not_ready = 0
    skipped_existing = 0
    built = 0
    failed = 0

    with get_session() as session:
        for c in candidates:
            checked += 1
            try:
                edition = (
                    session.query(TournamentEdition)
                    .options(joinedload(TournamentEdition.tournament))
                    .filter(TournamentEdition.id == c.edition_id)
                    .one()
                )

                # Extra eligibility safety (should already be filtered).
                if not edition.tournament or not is_forecast_eligible_tournament(edition.tournament):
                    print(f"- SKIP       edition_id={c.edition_id}  reason=not_eligible")
                    continue

                label = _fmt_edition(edition)

                ready = is_draw_forecast_ready(session, edition)
                if not ready:
                    not_ready += 1
                    print(f"- NOT READY  {label}")
                    continue

                if args.dry_run:
                    print(f"- WOULD BUILD {label}")
                    continue

                active_before = get_active_run(session, edition.id)
                t0 = perf_counter()
                run = build_forecast_run(
                    session,
                    edition_id=edition.id,
                    force=args.force,
                    build_reason="cron_daily",
                )
                session.commit()
                dt = perf_counter() - t0

                if active_before and run.id == active_before.id:
                    skipped_existing += 1
                    print(f"- SKIP       {label}  active_run_id={run.id}  no_change")
                else:
                    built += 1
                    print(f"- BUILT      {label}  run_id={run.id}  status={run.status}  {dt:.2f}s")

            except Exception as exc:
                failed += 1
                session.rollback()
                print(f"- FAILED     edition_id={c.edition_id}  error={exc}")

    elapsed = perf_counter() - started
    print("-" * 60)
    print(
        "Summary: "
        f"checked={checked} "
        f"not_ready={not_ready} "
        f"skipped_existing={skipped_existing} "
        f"built={built} "
        f"failed={failed} "
        f"elapsed={elapsed:.1f}s"
    )

    # Non-zero exit if any edition failed (helps cron alerting)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
