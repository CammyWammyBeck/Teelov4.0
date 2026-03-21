from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal
import math
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from teelo.db.models import (
    FeatureSet,
    Match,
    MatchFeatures,
    MatchSurfaceEloSnapshot,
    Player,
    Tournament,
    TournamentEdition,
)
from teelo.db.session import get_session
from teelo.elo.constants import get_level_code
from teelo.features.registry import FeatureRegistry
from teelo.features.state import MatchContext, MatchRecord, PlayerState

logger = structlog.get_logger(__name__)

TERMINAL_STATUSES = {"completed", "retired", "walkover", "default"}
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 10000
# Temporal orders at or above this threshold use the 9999-12-31 far-future
# sentinel date (assigned when no real date is available).  Exclude them from
# the incremental watermark so they don't permanently block new computations.
SENTINEL_TEMPORAL_ORDER = 999900000000000


class FeatureEngine:
    def __init__(self, registry: FeatureRegistry, feature_set_name: str) -> None:
        self.registry = registry
        self.feature_set_name = feature_set_name
        self.player_states: dict[int, PlayerState] = {}
        self._batch: list[dict[str, Any]] = []

    def run(self, backfill: bool = False) -> None:
        with get_session() as session:
            feature_set = self._get_or_create_feature_set(session)
            last_computed = self._get_last_computed_temporal_order(session, feature_set.id)

            logger.info(
                "feature_engine.start",
                feature_set=self.feature_set_name,
                feature_set_id=feature_set.id,
                backfill=backfill,
                last_computed_temporal_order=last_computed,
            )

            match_rows = self._load_matches(session)
            logger.info("feature_engine.matches_loaded", count=len(match_rows))

            if not match_rows:
                return

            # Load player nationalities for country performance features
            nationality_rows = session.execute(
                select(Player.id, Player.nationality_ioc)
            ).all()
            nationalities: dict[int, str | None] = {
                row.id: row.nationality_ioc for row in nationality_rows
            }

            # Replay always starts from the beginning in both modes to rebuild state.
            # In incremental mode, we only persist features for matches after the watermark
            # OR matches that don't have features yet (e.g. newly ingested with past dates).
            existing_feature_ids = self._load_existing_feature_match_ids(
                session, feature_set.id
            )
            snapshot_by_match_player = self._load_surface_snapshots(session, match_rows, backfill)

            processed = 0
            computed = 0
            updated_states = 0
            total = len(match_rows)
            t_start = time.monotonic()

            for row in match_rows:
                processed += 1

                if processed % PROGRESS_INTERVAL == 0:
                    elapsed = time.monotonic() - t_start
                    pct = processed / total * 100
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta_s = (total - processed) / rate if rate > 0 else 0
                    eta_min = eta_s / 60
                    logger.info(
                        "feature_engine.progress",
                        processed=processed,
                        total=total,
                        pct=f"{pct:.1f}%",
                        rate=f"{rate:.0f} matches/s",
                        eta=f"{eta_min:.1f} min",
                        computed=computed,
                    )

                state_a = self.player_states.setdefault(
                    row.player_a_id, PlayerState(player_id=row.player_a_id)
                )
                state_b = self.player_states.setdefault(
                    row.player_b_id, PlayerState(player_id=row.player_b_id)
                )

                surface = row.te_surface or row.t_surface
                level_code = get_level_code(row.level, row.tour)

                ctx = MatchContext(
                    match_id=row.id,
                    match_date=row.match_date,
                    surface=surface,
                    level_code=level_code,
                    tour=row.tour,
                    gender=row.gender,
                    round=row.round,
                    year=row.year,
                    seed_a=row.player_a_seed,
                    seed_b=row.player_b_seed,
                    temporal_order=row.temporal_order,
                    tournament_edition_id=row.tournament_edition_id,
                    tournament_id=row.tournament_id,
                    match_date_estimated=row.match_date_estimated,
                    tournament_country_ioc=getattr(row, "tournament_country_ioc", None),
                    player_a_nationality=nationalities.get(row.player_a_id),
                    player_b_nationality=nationalities.get(row.player_b_id),
                )

                should_compute = (
                    backfill
                    or last_computed is None
                    or row.temporal_order > last_computed
                    or row.id not in existing_feature_ids
                )
                if should_compute:
                    features = self.registry.compute_all(state_a, state_b, ctx)
                    self._batch.append(
                        {
                            "match_id": row.id,
                            "feature_set_id": feature_set.id,
                            "features": features,
                            "computed_at": datetime.utcnow(),
                        }
                    )
                    computed += 1

                if row.status in TERMINAL_STATUSES and row.winner_id is not None:
                    games_a, games_b = _compute_games(row.score_structured)
                    sets_a, sets_b, tiebreaks_a, tiebreaks_b = _compute_score_summary(
                        row.score_structured
                    )
                    player_a_won = row.winner_id == row.player_a_id

                    elo_pre_a = _to_float(row.elo_pre_player_a, default=state_a.elo_current)
                    elo_pre_b = _to_float(row.elo_pre_player_b, default=state_b.elo_current)
                    elo_post_a = _to_float(row.elo_post_player_a, default=state_a.elo_current)
                    elo_post_b = _to_float(row.elo_post_player_b, default=state_b.elo_current)

                    surface_a = snapshot_by_match_player.get((row.id, row.player_a_id))
                    surface_b = snapshot_by_match_player.get((row.id, row.player_b_id))
                    surface_elo_post_a = (
                        _to_float(surface_a[1], default=None) if surface_a is not None else None
                    )
                    surface_elo_post_b = (
                        _to_float(surface_b[1], default=None) if surface_b is not None else None
                    )
                    surface_elo_pre_a = (
                        _to_float(surface_a[0], default=None) if surface_a is not None else None
                    )
                    surface_elo_pre_b = (
                        _to_float(surface_b[0], default=None) if surface_b is not None else None
                    )

                    expected_a = _expected_win_probability(elo_pre_a, elo_pre_b)
                    expected_b = _expected_win_probability(elo_pre_b, elo_pre_a)
                    deciding_set_played = (sets_a + sets_b) >= 3 and abs(sets_a - sets_b) == 1
                    straight_sets_a = player_a_won and sets_b == 0 and sets_a >= 2
                    straight_sets_b = (not player_a_won) and sets_a == 0 and sets_b >= 2
                    close_match = deciding_set_played or (tiebreaks_a + tiebreaks_b) > 0

                    record_a = MatchRecord(
                        temporal_order=row.temporal_order,
                        won=player_a_won,
                        surface=surface,
                        level_code=level_code,
                        games_won=games_a,
                        games_lost=games_b,
                        tournament_edition_id=row.tournament_edition_id,
                        tournament_id=row.tournament_id,
                        match_date=row.match_date,
                        opponent_id=row.player_b_id,
                        opponent_elo=elo_pre_b,
                        opponent_surface_elo=surface_elo_pre_b,
                        expected_win_prob=expected_a,
                        sets_won=sets_a,
                        sets_lost=sets_b,
                        tiebreaks_played=tiebreaks_a + tiebreaks_b,
                        tiebreaks_won=tiebreaks_a,
                        deciding_set_played=deciding_set_played,
                        straight_sets=straight_sets_a,
                        close_match=close_match,
                    )
                    record_b = MatchRecord(
                        temporal_order=row.temporal_order,
                        won=not player_a_won,
                        surface=surface,
                        level_code=level_code,
                        games_won=games_b,
                        games_lost=games_a,
                        tournament_edition_id=row.tournament_edition_id,
                        tournament_id=row.tournament_id,
                        match_date=row.match_date,
                        opponent_id=row.player_a_id,
                        opponent_elo=elo_pre_a,
                        opponent_surface_elo=surface_elo_pre_a,
                        expected_win_prob=expected_b,
                        sets_won=sets_b,
                        sets_lost=sets_a,
                        tiebreaks_played=tiebreaks_a + tiebreaks_b,
                        tiebreaks_won=tiebreaks_b,
                        deciding_set_played=deciding_set_played,
                        straight_sets=straight_sets_b,
                        close_match=close_match,
                    )

                    state_a.update(record_a, elo_post_a, surface_elo_post_a)
                    state_b.update(record_b, elo_post_b, surface_elo_post_b)
                    updated_states += 1

                if len(self._batch) >= BATCH_SIZE:
                    self._flush_batch(session, feature_set.id)

            self._flush_batch(session, feature_set.id)

            logger.info(
                "feature_engine.done",
                matches_processed=processed,
                features_computed=computed,
                state_updates=updated_states,
                players_in_state=len(self.player_states),
            )

    def _get_or_create_feature_set(self, session: Session) -> FeatureSet:
        feature_set = session.execute(
            select(FeatureSet).where(FeatureSet.name == self.feature_set_name)
        ).scalar_one_or_none()

        if feature_set is not None:
            return feature_set

        feature_set = FeatureSet(
            name=self.feature_set_name,
            version="1",
            description=f"Autogenerated feature set for {self.feature_set_name}",
            feature_definitions=self.registry.all_feature_names(),
        )
        session.add(feature_set)
        session.flush()

        logger.info(
            "feature_engine.feature_set_created",
            feature_set=self.feature_set_name,
            feature_set_id=feature_set.id,
            feature_count=len(self.registry.all_feature_names()),
        )
        return feature_set

    def _get_last_computed_temporal_order(
        self, session: Session, feature_set_id: int
    ) -> int | None:
        return session.execute(
            select(func.max(Match.temporal_order))
            .select_from(Match)
            .join(MatchFeatures, MatchFeatures.match_id == Match.id)
            .where(MatchFeatures.feature_set_id == feature_set_id)
            .where(Match.temporal_order < SENTINEL_TEMPORAL_ORDER)
        ).scalar_one()

    def _load_existing_feature_match_ids(
        self, session: Session, feature_set_id: int
    ) -> set[int]:
        """Return the set of match IDs that already have features for this set."""
        rows = session.execute(
            select(MatchFeatures.match_id).where(
                MatchFeatures.feature_set_id == feature_set_id
            )
        ).scalars().all()
        return set(rows)

    def _load_matches(self, session: Session) -> list[Any]:
        stmt = (
            select(
                Match.id,
                Match.player_a_id,
                Match.player_b_id,
                Match.winner_id,
                Match.temporal_order,
                Match.match_date,
                Match.round,
                Match.status,
                Match.score_structured,
                Match.player_a_seed,
                Match.player_b_seed,
                Match.tournament_edition_id,
                Match.match_date_estimated,
                Match.elo_pre_player_a,
                Match.elo_pre_player_b,
                Match.elo_post_player_a,
                Match.elo_post_player_b,
                TournamentEdition.year,
                TournamentEdition.tournament_id,
                TournamentEdition.surface.label("te_surface"),
                Tournament.surface.label("t_surface"),
                Tournament.tour,
                Tournament.gender,
                Tournament.level,
                Tournament.country_ioc.label("tournament_country_ioc"),
            )
            .select_from(Match)
            .join(TournamentEdition, TournamentEdition.id == Match.tournament_edition_id)
            .join(Tournament, Tournament.id == TournamentEdition.tournament_id)
            .where(Match.temporal_order.is_not(None))
            .order_by(Match.temporal_order.asc())
        )
        return list(session.execute(stmt).all())

    def _load_surface_snapshots(
        self,
        session: Session,
        match_rows: list[Any],
        backfill: bool,
    ) -> dict[tuple[int, int], tuple[Decimal, Decimal]]:
        stmt = select(
            MatchSurfaceEloSnapshot.match_id,
            MatchSurfaceEloSnapshot.player_id,
            MatchSurfaceEloSnapshot.elo_pre,
            MatchSurfaceEloSnapshot.elo_post,
        )

        if not backfill:
            match_ids = [row.id for row in match_rows]
            if not match_ids:
                return {}
            stmt = stmt.where(MatchSurfaceEloSnapshot.match_id.in_(match_ids))

        snapshots: dict[tuple[int, int], tuple[Decimal, Decimal]] = {}
        for row in session.execute(stmt):
            snapshots[(row.match_id, row.player_id)] = (row.elo_pre, row.elo_post)

        logger.info(
            "feature_engine.surface_snapshots_loaded", count=len(snapshots), backfill=backfill
        )
        return snapshots

    def _flush_batch(self, session: Session, feature_set_id: int) -> None:
        if not self._batch:
            return

        rows = [
            {
                "match_id": row["match_id"],
                "feature_set_id": row.get("feature_set_id", feature_set_id),
                "features": row["features"],
                "computed_at": row.get("computed_at", datetime.utcnow()),
            }
            for row in self._batch
        ]

        stmt = insert(MatchFeatures).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_match_features",
            set_={
                "features": stmt.excluded.features,
                "computed_at": stmt.excluded.computed_at,
            },
        )
        session.execute(stmt)

        logger.info("feature_engine.batch_flushed", rows=len(rows))
        self._batch.clear()


def _to_float(value: Decimal | float | int | None, default: float | None) -> float | None:
    if value is None:
        return default
    return float(value)


def _compute_games(score_structured: Any) -> tuple[int, int]:
    if not isinstance(score_structured, list):
        return 0, 0

    games_a = 0
    games_b = 0

    for item in score_structured:
        if not isinstance(item, dict):
            continue

        set_a = item.get("a", 0)
        set_b = item.get("b", 0)

        games_a += int(set_a) if isinstance(set_a, (int, float, Decimal)) else 0
        games_b += int(set_b) if isinstance(set_b, (int, float, Decimal)) else 0

    return games_a, games_b


def _compute_score_summary(score_structured: Any) -> tuple[int, int, int, int]:
    if not isinstance(score_structured, list):
        return 0, 0, 0, 0

    sets_a = 0
    sets_b = 0
    tiebreaks_a = 0
    tiebreaks_b = 0

    for item in score_structured:
        if not isinstance(item, dict):
            continue

        set_a = item.get("a", 0)
        set_b = item.get("b", 0)
        if isinstance(set_a, (int, float, Decimal)) and isinstance(set_b, (int, float, Decimal)):
            if set_a > set_b:
                sets_a += 1
            elif set_b > set_a:
                sets_b += 1

        tb_a = item.get("tb_a")
        tb_b = item.get("tb_b")
        if isinstance(tb_a, (int, float, Decimal)) and isinstance(tb_b, (int, float, Decimal)):
            if tb_a > tb_b:
                tiebreaks_a += 1
            elif tb_b > tb_a:
                tiebreaks_b += 1

    return sets_a, sets_b, tiebreaks_a, tiebreaks_b


def _expected_win_probability(player_elo: float | None, opponent_elo: float | None) -> float | None:
    if player_elo is None or opponent_elo is None:
        return None
    return 1.0 / (1.0 + math.pow(10.0, (opponent_elo - player_elo) / 400.0))


if __name__ == "__main__":
    import argparse

    from teelo.features import build_registry, latest_preset

    parser = argparse.ArgumentParser(
        description="Compute and store match features in chronological order"
    )
    parser.add_argument(
        "--backfill", action="store_true", help="Recompute and overwrite features for all matches"
    )
    parser.add_argument(
        "--preset",
        default=None,
        help="Feature preset name (default: latest preset)",
    )
    parser.add_argument(
        "--feature-set",
        default=None,
        help="Feature set name (default: same as preset)",
    )
    args = parser.parse_args()

    preset = args.preset or latest_preset()
    feature_set_name = args.feature_set or preset

    registry = build_registry(preset)
    engine = FeatureEngine(registry=registry, feature_set_name=feature_set_name)
    engine.run(backfill=args.backfill)
