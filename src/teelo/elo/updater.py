"""
ELO update service — processes unprocessed terminal matches and keeps ratings current.

This is the single source of truth for incremental ELO updates. It replaces
both live.py (inline scraper updates) and update_elo_incremental.py (batch script)
with one clean class that both call sites use.

Normal flow (new matches appended in chronological order):
1. Load player states from DB for involved players (one bulk query)
2. Find unprocessed terminal matches sorted by temporal_order (one query with JOIN)
3. Process in memory — no DB calls per match
4. Bulk write: match ELO columns + PlayerEloState upsert
5. Refresh pre-match snapshots for upcoming/scheduled matches (one UPDATE)

Backfill flow (rare — historical matches inserted before already-processed ones):
- Detected when any unprocessed match has temporal_order < player's last_temporal_order
- Handled by clearing all match ELO from the backfill point forward, recovering
  player states from stored elo_post values as anchors, then reprocessing everything
  from that point forward in a single pass.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Callable
from typing import NamedTuple

from sqlalchemy import delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from teelo.db.models import (
    Match,
    MatchSurfaceEloSnapshot,
    PlayerEloState,
    PlayerSurfaceEloState,
    Tournament,
    TournamentEdition,
)
from teelo.elo.boost import calculate_k_boost
from teelo.elo.calculator import calculate_fast
from teelo.elo.constants import get_level_code
from teelo.elo.decay import apply_inactivity_decay
from teelo.elo.margin import calculate_margin_multiplier
from teelo.elo.params_store import get_active_elo_params
from teelo.elo.pipeline import EloParams, date_from_temporal_order, initial_elo_for_level_code

# Terminal match statuses that receive ELO computation
TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")


def resolve_surface_bucket(
    edition_surface: str | None,
    tournament_surface: str | None,
    indoor_outdoor: str | None,
) -> str:
    """
    Resolve a match into one of the player-page surface buckets.

    Priority:
    1) Indoor events are bucketed as "Indoor"
    2) Otherwise normalize edition/tournament surface to Hard/Clay/Grass
    3) Unknown values default to Hard for conservative baseline behavior
    """
    if indoor_outdoor and indoor_outdoor.strip().lower() == "indoor":
        return "Indoor"

    raw_surface = (edition_surface or tournament_surface or "").strip().lower()
    if raw_surface == "hard":
        return "Hard"
    if raw_surface == "clay":
        return "Clay"
    if raw_surface == "grass":
        return "Grass"
    return "Hard"


# ---------------------------------------------------------------------------
# Internal data structures — lightweight, avoid ORM overhead in the hot path
# ---------------------------------------------------------------------------

@dataclass
class _PlayerState:
    """In-memory ELO state for one player during a processing run."""
    player_id: int
    rating: float = 1500.0
    match_count: int = 0
    last_temporal_order: int | None = None
    last_match_date: date | None = None
    career_peak: float = 1500.0


@dataclass
class _SurfaceState:
    """In-memory surface ELO state for one player/surface pair."""
    player_id: int
    surface: str
    rating: float = 1500.0
    match_count: int = 0
    last_temporal_order: int | None = None
    last_match_date: date | None = None
    career_peak: float = 1500.0


class _MatchRow(NamedTuple):
    """Lightweight match record loaded from DB for processing."""
    id: int
    player_a_id: int
    player_b_id: int
    winner_id: int
    temporal_order: int
    match_date: date | None
    score_structured: dict | None
    level_code: str
    surface: str


class _MatchUpdate(NamedTuple):
    """ELO values to write back to the matches table after processing."""
    match_id: int
    elo_pre_player_a: float
    elo_pre_player_b: float
    elo_post_player_a: float
    elo_post_player_b: float


class _SurfaceSnapshotUpdate(NamedTuple):
    """Surface ELO snapshot values to write after processing."""
    match_id: int
    player_id: int
    surface: str
    temporal_order: int
    elo_pre: float
    elo_post: float
    elo_change: float


@dataclass
class UpdateResult:
    """Summary returned by EloUpdater.run() or EloUpdater.rebuild()."""
    processed: int = 0
    backfill_triggered: bool = False
    backfill_temporal: int | None = None
    pre_snapshots_refreshed: int = 0


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------

class EloUpdater:
    """
    Processes unprocessed terminal matches and updates ELO values.

    Usage — fast inline post-scrape update (only processes relevant players):

        updater = EloUpdater.from_session(session)
        result = updater.run(session, player_ids={player_a_id, player_b_id, ...})
        session.commit()

    Usage — hourly pipeline / full scan (catches anything missed):

        updater = EloUpdater.from_session(session)
        result = updater.run(session)  # player_ids=None scans all unprocessed

    Usage — full rebuild after param change or data corruption:

        updater = EloUpdater(params, params_version)
        result = updater.rebuild(session)
        session.commit()
    """

    def __init__(self, params: EloParams, params_version: str) -> None:
        self.params = params
        self.params_version = params_version

    @classmethod
    def from_session(cls, session: Session) -> "EloUpdater":
        """Instantiate using the currently active ELO parameter set from the DB."""
        params, version = get_active_elo_params(session)
        return cls(params=params, params_version=version)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        session: Session,
        player_ids: set[int] | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
        progress_every: int = 0,
        batch_size: int = 10000,
        commit_every_batch: bool = False,
    ) -> UpdateResult:
        """
        Process all unprocessed terminal matches and update ELO.

        Args:
            session: Active SQLAlchemy session. Caller is responsible for commit.
            player_ids: Optional player ID filter for fast post-scrape updates.
                        When provided, only matches involving these players are
                        queried. Pass None (default) for a full scan of all
                        unprocessed matches — used by the hourly pipeline.

        Returns:
            UpdateResult with counts and backfill info.
        """
        result = UpdateResult()

        if batch_size <= 0:
            batch_size = 10000

        # 1. Find unprocessed terminal matches (with tournament level via JOIN)
        total_pending = self._count_unprocessed(session, player_ids)
        unprocessed = self._find_unprocessed(session, player_ids, limit=batch_size)
        if not unprocessed:
            return result

        # 2. Load player states for all involved players in one bulk query
        involved_ids = (
            {m.player_a_id for m in unprocessed}
            | {m.player_b_id for m in unprocessed}
        )
        states = self._load_player_states(session, involved_ids)
        surface_states = self._load_surface_states(session, involved_ids)

        # 3. Backfill detection: is any unprocessed match earlier than a player's
        #    last processed match? (Only happens when historical data is scraped.)
        backfill_temporal = self._find_backfill_point(unprocessed, states)

        if backfill_temporal is not None:
            result.backfill_triggered = True
            result.backfill_temporal = backfill_temporal
            print(
                f"[EloUpdater] Backfill detected at temporal_order={backfill_temporal}. "
                "Clearing affected matches and recovering historical states."
            )
            # Clears match ELO from backfill_temporal forward and resets player states
            self._handle_backfill(session, backfill_temporal, states, surface_states)

            # Re-scan — the clear may have exposed many more unprocessed matches
            unprocessed = self._find_unprocessed(session, player_ids=None, limit=batch_size)
            if not unprocessed:
                return result

            # Load states for any newly-involved players not already in our dict.
            # After a backfill, PlayerEloState still holds current ELO — recover
            # pre-backfill state from match history instead.
            all_involved = (
                {m.player_a_id for m in unprocessed}
                | {m.player_b_id for m in unprocessed}
            )
            new_ids = all_involved - set(states.keys())
            if new_ids:
                self._recover_states_before(
                    session, list(new_ids), backfill_temporal, states, surface_states
                )

        # 4. Process/write in bounded batches to avoid giant statements.
        touched_all: set[int] = set()
        processed_so_far = 0
        while unprocessed:
            all_involved = (
                {m.player_a_id for m in unprocessed}
                | {m.player_b_id for m in unprocessed}
            )
            new_ids = all_involved - set(states.keys())
            if new_ids:
                if backfill_temporal is not None:
                    # After a backfill, PlayerEloState still holds current ELO;
                    # recover the correct pre-backfill state from match history.
                    self._recover_states_before(
                        session, list(new_ids), backfill_temporal, states, surface_states
                    )
                else:
                    states.update(self._load_player_states(session, new_ids))
                    surface_states.update(self._load_surface_states(session, new_ids))

            def _batch_progress(done_in_batch: int, total_in_batch: int) -> None:
                if progress_callback is None:
                    return
                progress_callback(processed_so_far + done_in_batch, total_pending)

            (
                match_updates,
                touched_ids,
                surface_snapshots,
                touched_surface_keys,
            ) = self._process_matches(
                unprocessed,
                states,
                surface_states,
                progress_callback=_batch_progress if progress_callback else None,
                progress_every=progress_every,
            )
            if not match_updates:
                break

            self._bulk_write(
                session,
                match_updates,
                states,
                touched_ids,
                surface_states,
                surface_snapshots,
                touched_surface_keys,
            )
            if commit_every_batch:
                session.commit()
            else:
                session.flush()

            processed_so_far += len(match_updates)
            touched_all.update(touched_ids)
            result.processed += len(match_updates)

            if len(unprocessed) < batch_size:
                break
            unprocessed = self._find_unprocessed(session, player_ids, limit=batch_size)

        # 6. Refresh elo_pre snapshots once at end for all touched players
        if touched_all:
            result.pre_snapshots_refreshed = self._refresh_pre_snapshots(session, touched_all)

        return result

    def rebuild(
        self,
        session: Session,
        progress_callback: Callable[[int, int], None] | None = None,
        progress_every: int = 0,
        batch_size: int = 10000,
        commit_every_batch: bool = False,
    ) -> UpdateResult:
        """
        Full rebuild: wipe all ELO data and reprocess every terminal match
        from scratch in temporal order.

        Use after changing ELO parameters (optimise_elo.py --activate-best) or
        if player_elo_states has become corrupted.

        Args:
            session: Active SQLAlchemy session. Caller is responsible for commit.
        """
        # Clear all existing ELO data
        session.query(PlayerEloState).delete()
        session.query(PlayerSurfaceEloState).delete()
        session.query(MatchSurfaceEloSnapshot).delete()
        session.execute(
            update(Match)
            .values(
                elo_pre_player_a=None,
                elo_pre_player_b=None,
                elo_post_player_a=None,
                elo_post_player_b=None,
                elo_params_version=None,
                elo_processed_at=None,
                elo_needs_recompute=False,
            )
            .execution_options(synchronize_session=False)
        )
        session.flush()

        if batch_size <= 0:
            batch_size = 10000

        # Process everything — states dict starts empty (all players at default)
        total_pending = self._count_unprocessed(session, player_ids=None)
        unprocessed = self._find_unprocessed(session, player_ids=None, limit=batch_size)
        if not unprocessed or total_pending == 0:
            return UpdateResult()
        states: dict[int, _PlayerState] = {}
        surface_states: dict[tuple[int, str], _SurfaceState] = {}
        touched_all: set[int] = set()
        processed_so_far = 0

        while unprocessed:
            def _batch_progress(done_in_batch: int, total_in_batch: int) -> None:
                if progress_callback is None:
                    return
                progress_callback(processed_so_far + done_in_batch, total_pending)

            (
                match_updates,
                touched_ids,
                surface_snapshots,
                touched_surface_keys,
            ) = self._process_matches(
                unprocessed,
                states,
                surface_states,
                progress_callback=_batch_progress if progress_callback else None,
                progress_every=progress_every,
            )
            if not match_updates:
                break

            self._bulk_write(
                session,
                match_updates,
                states,
                touched_ids,
                surface_states,
                surface_snapshots,
                touched_surface_keys,
            )
            if commit_every_batch:
                session.commit()
            else:
                session.flush()

            processed_so_far += len(match_updates)
            touched_all.update(touched_ids)

            if len(unprocessed) < batch_size:
                break
            unprocessed = self._find_unprocessed(session, player_ids=None, limit=batch_size)

        n_pre = self._refresh_pre_snapshots(session, touched_all) if touched_all else 0
        return UpdateResult(processed=processed_so_far, pre_snapshots_refreshed=n_pre)

    def run_for_match_ids(
        self,
        session: Session,
        match_ids: list[int],
        progress_callback: Callable[[int, int], None] | None = None,
        progress_every: int = 0,
    ) -> UpdateResult:
        """
        Process specific match IDs inline after scraping.

        Unlike run(), this skips the full table scan and backfill detection.
        The invariant is: these matches were just scraped and are the most
        recent for the involved players. The hourly update_elo.py catches
        any edge cases (out-of-order historical inserts, etc.).

        Args:
            session: Active SQLAlchemy session. Caller is responsible for commit.
            match_ids: IDs of terminal matches to process.
                       Typically from ResultsIngestionStats.completed_match_ids.

        Returns:
            UpdateResult with counts.
        """
        if not match_ids:
            return UpdateResult()

        matches = self._find_by_ids(session, match_ids)
        if not matches:
            return UpdateResult()

        involved_ids = (
            {m.player_a_id for m in matches}
            | {m.player_b_id for m in matches}
        )
        states = self._load_player_states(session, involved_ids)
        surface_states = self._load_surface_states(session, involved_ids)
        (
            match_updates,
            touched_ids,
            surface_snapshots,
            touched_surface_keys,
        ) = self._process_matches(
            matches,
            states,
            surface_states,
            progress_callback=progress_callback,
            progress_every=progress_every,
        )

        if not match_updates:
            return UpdateResult()

        self._bulk_write(
            session,
            match_updates,
            states,
            touched_ids,
            surface_states,
            surface_snapshots,
            touched_surface_keys,
        )
        n_pre = self._refresh_pre_snapshots(session, touched_ids)
        return UpdateResult(processed=len(match_updates), pre_snapshots_refreshed=n_pre)

    # ------------------------------------------------------------------
    # DB queries
    # ------------------------------------------------------------------

    def _find_unprocessed(
        self,
        session: Session,
        player_ids: set[int] | None,
        limit: int | None = None,
    ) -> list[_MatchRow]:
        """
        Load all unprocessed terminal matches with tournament level info.

        "Unprocessed" = elo_post IS NULL for either player, or elo_needs_recompute=True.
        Results are sorted by (temporal_order ASC, id ASC) for sequential processing.

        Args:
            player_ids: When provided, narrows the query to matches involving
                        those players (fast path). None = full table scan.
        """
        stmt = (
            select(
                Match.id,
                Match.player_a_id,
                Match.player_b_id,
                Match.winner_id,
                Match.temporal_order,
                Match.match_date,
                Match.score_structured,
                TournamentEdition.surface.label("edition_surface"),
                Tournament.surface.label("tournament_surface"),
                Tournament.indoor_outdoor,
                Tournament.level,
                Tournament.tour,
            )
            .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
            .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
            .where(Match.status.in_(TERMINAL_STATUSES))
            .where(Match.winner_id.isnot(None))
            .where(Match.temporal_order.isnot(None))
            .where(
                or_(
                    Match.elo_post_player_a.is_(None),
                    Match.elo_post_player_b.is_(None),
                    Match.elo_needs_recompute.is_(True),
                )
            )
        )

        # Narrow to specific players for the fast post-scrape path
        if player_ids:
            stmt = stmt.where(
                or_(
                    Match.player_a_id.in_(player_ids),
                    Match.player_b_id.in_(player_ids),
                )
            )

        stmt = stmt.order_by(Match.temporal_order.asc(), Match.id.asc())
        if limit is not None:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()

        result: list[_MatchRow] = []
        for row in rows:
            level_code = get_level_code(row.level, row.tour)
            # Use stored match_date if available; extract from temporal_order otherwise
            match_date = row.match_date
            if match_date is None and row.temporal_order is not None:
                match_date = date_from_temporal_order(row.temporal_order)
            result.append(
                _MatchRow(
                    id=row.id,
                    player_a_id=row.player_a_id,
                    player_b_id=row.player_b_id,
                    winner_id=row.winner_id,
                    temporal_order=row.temporal_order,
                    match_date=match_date,
                    score_structured=row.score_structured,
                    level_code=level_code,
                    surface=resolve_surface_bucket(
                        row.edition_surface,
                        row.tournament_surface,
                        row.indoor_outdoor,
                    ),
                )
            )
        return result

    def _count_unprocessed(self, session: Session, player_ids: set[int] | None) -> int:
        """Count unprocessed terminal matches for progress and batch planning."""
        stmt = (
            select(func.count())
            .where(Match.status.in_(TERMINAL_STATUSES))
            .where(Match.winner_id.isnot(None))
            .where(Match.temporal_order.isnot(None))
            .where(
                or_(
                    Match.elo_post_player_a.is_(None),
                    Match.elo_post_player_b.is_(None),
                    Match.elo_needs_recompute.is_(True),
                )
            )
        )
        if player_ids:
            stmt = stmt.where(
                or_(
                    Match.player_a_id.in_(player_ids),
                    Match.player_b_id.in_(player_ids),
                )
            )
        return int(session.execute(stmt).scalar() or 0)

    def _find_by_ids(
        self,
        session: Session,
        match_ids: list[int],
    ) -> list[_MatchRow]:
        """
        Load specific matches by ID with tournament level info.

        Structurally identical to _find_unprocessed but filters by explicit
        match IDs instead of scanning for all unprocessed. Keeps the
        elo_post IS NULL / elo_needs_recompute guard so calling
        run_for_match_ids() is idempotent — matches already processed are
        silently skipped.

        Results are sorted by temporal_order ASC, id ASC so QF is processed
        before SF within the same tournament.
        """
        stmt = (
            select(
                Match.id,
                Match.player_a_id,
                Match.player_b_id,
                Match.winner_id,
                Match.temporal_order,
                Match.match_date,
                Match.score_structured,
                TournamentEdition.surface.label("edition_surface"),
                Tournament.surface.label("tournament_surface"),
                Tournament.indoor_outdoor,
                Tournament.level,
                Tournament.tour,
            )
            .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
            .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
            .where(Match.id.in_(match_ids))
            .where(Match.status.in_(TERMINAL_STATUSES))
            .where(Match.winner_id.isnot(None))
            .where(Match.temporal_order.isnot(None))
            .where(
                or_(
                    Match.elo_post_player_a.is_(None),
                    Match.elo_post_player_b.is_(None),
                    Match.elo_needs_recompute.is_(True),
                )
            )
            .order_by(Match.temporal_order.asc(), Match.id.asc())
        )
        rows = session.execute(stmt).all()

        result: list[_MatchRow] = []
        for row in rows:
            level_code = get_level_code(row.level, row.tour)
            match_date = row.match_date
            if match_date is None and row.temporal_order is not None:
                match_date = date_from_temporal_order(row.temporal_order)
            result.append(
                _MatchRow(
                    id=row.id,
                    player_a_id=row.player_a_id,
                    player_b_id=row.player_b_id,
                    winner_id=row.winner_id,
                    temporal_order=row.temporal_order,
                    match_date=match_date,
                    score_structured=row.score_structured,
                    level_code=level_code,
                    surface=resolve_surface_bucket(
                        row.edition_surface,
                        row.tournament_surface,
                        row.indoor_outdoor,
                    ),
                )
            )
        return result

    def _load_player_states(
        self,
        session: Session,
        player_ids: set[int],
    ) -> dict[int, _PlayerState]:
        """
        Bulk load PlayerEloState rows for a set of player IDs in one query.

        Players not yet in the DB (brand-new players) get a default
        _PlayerState(rating=1500.0). Their initial rating will be corrected
        during _process_matches based on the first tournament level they appear in.
        """
        if not player_ids:
            return {}

        rows = (
            session.query(PlayerEloState)
            .filter(PlayerEloState.player_id.in_(player_ids))
            .all()
        )

        states: dict[int, _PlayerState] = {
            row.player_id: _PlayerState(
                player_id=row.player_id,
                rating=float(row.rating),
                match_count=row.match_count,
                last_temporal_order=row.last_temporal_order,
                last_match_date=row.last_match_date,
                career_peak=float(row.career_peak),
            )
            for row in rows
        }

        # Default state for players with no existing ELO record
        for pid in player_ids:
            if pid not in states:
                states[pid] = _PlayerState(player_id=pid)

        return states

    def _load_surface_states(
        self,
        session: Session,
        player_ids: set[int],
    ) -> dict[tuple[int, str], _SurfaceState]:
        """Bulk load surface ELO states for players in one query."""
        if not player_ids:
            return {}

        rows = (
            session.query(PlayerSurfaceEloState)
            .filter(PlayerSurfaceEloState.player_id.in_(player_ids))
            .all()
        )

        return {
            (row.player_id, row.surface): _SurfaceState(
                player_id=row.player_id,
                surface=row.surface,
                rating=float(row.rating),
                match_count=row.match_count,
                last_temporal_order=row.last_temporal_order,
                last_match_date=row.last_match_date,
                career_peak=float(row.career_peak),
            )
            for row in rows
        }

    # ------------------------------------------------------------------
    # Backfill handling
    # ------------------------------------------------------------------

    def _find_backfill_point(
        self,
        unprocessed: list[_MatchRow],
        states: dict[int, _PlayerState],
    ) -> int | None:
        """
        Scan unprocessed matches for a backfill: any match whose temporal_order
        is earlier than the last match already processed for one of its players.

        Returns the earliest such temporal_order, or None if no backfill.

        This is an in-memory check — no DB calls.
        """
        earliest: int | None = None
        for match in unprocessed:
            state_a = states.get(match.player_a_id)
            state_b = states.get(match.player_b_id)
            is_backfill = (
                (
                    state_a
                    and state_a.last_temporal_order is not None
                    and match.temporal_order < state_a.last_temporal_order
                )
                or (
                    state_b
                    and state_b.last_temporal_order is not None
                    and match.temporal_order < state_b.last_temporal_order
                )
            )
            if is_backfill:
                if earliest is None or match.temporal_order < earliest:
                    earliest = match.temporal_order
        return earliest

    def _handle_backfill(
        self,
        session: Session,
        backfill_temporal: int,
        states: dict[int, _PlayerState],
        surface_states: dict[tuple[int, str], _SurfaceState],
    ) -> None:
        """
        Prepare for reprocessing from a backfill point.

        Step 1: Clear elo_post/elo_pre from ALL matches at or after backfill_temporal.
                We clear globally (not just the directly affected players) because
                any match after this point could have been influenced via the chain
                of rating changes — ELO is sequential, so changes propagate forward.

        Step 2: For each player whose last_temporal_order >= backfill_temporal, recover
                their ELO state from just before backfill_temporal using stored
                elo_post values as anchors. Uses a single DISTINCT ON query instead
                of N per-player queries.
        """
        # Step 1 — wipe all match ELO from the backfill point forward
        session.execute(
            update(Match)
            .where(Match.temporal_order >= backfill_temporal)
            .values(
                elo_post_player_a=None,
                elo_post_player_b=None,
                elo_pre_player_a=None,
                elo_pre_player_b=None,
                elo_needs_recompute=False,
                elo_processed_at=None,
            )
            .execution_options(synchronize_session=False)
        )
        session.execute(
            delete(MatchSurfaceEloSnapshot)
            .where(MatchSurfaceEloSnapshot.temporal_order >= backfill_temporal)
            .execution_options(synchronize_session=False)
        )

        # Step 2 — identify players whose state needs to be rolled back
        affected_ids = [
            pid
            for pid, state in states.items()
            if state.last_temporal_order is not None
            and state.last_temporal_order >= backfill_temporal
        ]
        if not affected_ids:
            return

        # Delete stale PlayerSurfaceEloState rows before recovering surface states.
        session.execute(
            delete(PlayerSurfaceEloState)
            .where(PlayerSurfaceEloState.player_id.in_(affected_ids))
            .execution_options(synchronize_session=False)
        )

        # Recover overall and surface ELO states from match history.
        self._recover_states_before(
            session, affected_ids, backfill_temporal, states, surface_states
        )

    def _recover_states_before(
        self,
        session: Session,
        player_ids: list[int],
        backfill_temporal: int,
        states: dict[int, _PlayerState],
        surface_states: dict[tuple[int, str], _SurfaceState],
    ) -> None:
        """
        Populate states/surface_states with each player's ELO as of just before
        backfill_temporal, recovered from stored elo_post values on match rows.

        Used when players are first encountered after a backfill has already been
        handled, so PlayerEloState (which still holds current ELO) cannot be
        trusted as a starting point.  Read-only: does not modify the database.

        For players with no processed matches before backfill_temporal, falls back
        to a default _PlayerState (rating=1500).
        """
        if not player_ids:
            return

        # Recover each player's last ELO before the backfill point.
        # DISTINCT ON (player_id) with ORDER BY temporal_order DESC gives us
        # the most recent prior match per player in one round trip.
        recovery_sql = text("""
            SELECT DISTINCT ON (player_id)
                player_id,
                elo_post,
                match_date,
                temporal_order
            FROM (
                SELECT
                    m.player_a_id         AS player_id,
                    m.elo_post_player_a   AS elo_post,
                    m.match_date,
                    m.temporal_order,
                    m.id                  AS match_id
                FROM matches m
                WHERE m.player_a_id = ANY(:player_ids)
                  AND m.temporal_order < :backfill_temporal
                  AND m.elo_post_player_a IS NOT NULL
                UNION ALL
                SELECT
                    m.player_b_id         AS player_id,
                    m.elo_post_player_b   AS elo_post,
                    m.match_date,
                    m.temporal_order,
                    m.id                  AS match_id
                FROM matches m
                WHERE m.player_b_id = ANY(:player_ids)
                  AND m.temporal_order < :backfill_temporal
                  AND m.elo_post_player_b IS NOT NULL
            ) sub
            ORDER BY player_id, temporal_order DESC, match_id DESC
        """)

        # Count total terminal matches before the backfill point per player.
        # Needed for K-boost (new-player boost fades over ~30 matches).
        count_sql = text("""
            SELECT player_id, COUNT(*) AS match_count
            FROM (
                SELECT player_a_id AS player_id
                FROM matches
                WHERE player_a_id = ANY(:player_ids)
                  AND temporal_order < :backfill_temporal
                  AND status = ANY(:terminal_statuses)
                UNION ALL
                SELECT player_b_id AS player_id
                FROM matches
                WHERE player_b_id = ANY(:player_ids)
                  AND temporal_order < :backfill_temporal
                  AND status = ANY(:terminal_statuses)
            ) sub
            GROUP BY player_id
        """)
        peak_sql = text("""
            SELECT player_id, MAX(elo_post) AS career_peak
            FROM (
                SELECT
                    m.player_a_id       AS player_id,
                    m.elo_post_player_a AS elo_post
                FROM matches m
                WHERE m.player_a_id = ANY(:player_ids)
                  AND m.temporal_order < :backfill_temporal
                  AND m.elo_post_player_a IS NOT NULL
                UNION ALL
                SELECT
                    m.player_b_id       AS player_id,
                    m.elo_post_player_b AS elo_post
                FROM matches m
                WHERE m.player_b_id = ANY(:player_ids)
                  AND m.temporal_order < :backfill_temporal
                  AND m.elo_post_player_b IS NOT NULL
            ) sub
            GROUP BY player_id
        """)

        recovery_rows = session.execute(
            recovery_sql,
            {"player_ids": player_ids, "backfill_temporal": backfill_temporal},
        ).all()
        count_rows = session.execute(
            count_sql,
            {
                "player_ids": player_ids,
                "backfill_temporal": backfill_temporal,
                "terminal_statuses": list(TERMINAL_STATUSES),
            },
        ).all()
        peak_rows = session.execute(
            peak_sql,
            {"player_ids": player_ids, "backfill_temporal": backfill_temporal},
        ).all()

        count_by_player = {row.player_id: int(row.match_count) for row in count_rows}
        peak_by_player = {row.player_id: float(row.career_peak) for row in peak_rows}
        recovered_pids: set[int] = set()

        for row in recovery_rows:
            pid = row.player_id
            rating = float(row.elo_post)
            states[pid] = _PlayerState(
                player_id=pid,
                rating=rating,
                match_count=count_by_player.get(pid, 0),
                last_temporal_order=int(row.temporal_order),
                last_match_date=row.match_date,
                career_peak=peak_by_player.get(pid, rating),
            )
            recovered_pids.add(pid)

        # Players with no prior matches before the backfill point start from scratch.
        for pid in player_ids:
            if pid not in recovered_pids:
                states[pid] = _PlayerState(player_id=pid)

        # Surface state rollback: clear any existing in-memory entries for these
        # players, then repopulate from the latest snapshot before backfill_temporal.
        for key in list(surface_states.keys()):
            if key[0] in set(player_ids):
                del surface_states[key]

        surface_recovery_sql = text("""
            SELECT DISTINCT ON (player_id, surface)
                player_id,
                surface,
                elo_post,
                temporal_order,
                processed_at
            FROM match_surface_elo_snapshots
            WHERE player_id = ANY(:player_ids)
              AND temporal_order < :backfill_temporal
            ORDER BY player_id, surface, temporal_order DESC, processed_at DESC
        """)

        surface_count_sql = text("""
            SELECT player_id, surface, COUNT(*) AS match_count
            FROM match_surface_elo_snapshots
            WHERE player_id = ANY(:player_ids)
              AND temporal_order < :backfill_temporal
            GROUP BY player_id, surface
        """)
        surface_peak_sql = text("""
            SELECT player_id, surface, MAX(elo_post) AS career_peak
            FROM match_surface_elo_snapshots
            WHERE player_id = ANY(:player_ids)
              AND temporal_order < :backfill_temporal
            GROUP BY player_id, surface
        """)

        surface_recovery_rows = session.execute(
            surface_recovery_sql,
            {"player_ids": player_ids, "backfill_temporal": backfill_temporal},
        ).all()
        surface_count_rows = session.execute(
            surface_count_sql,
            {"player_ids": player_ids, "backfill_temporal": backfill_temporal},
        ).all()
        surface_peak_rows = session.execute(
            surface_peak_sql,
            {"player_ids": player_ids, "backfill_temporal": backfill_temporal},
        ).all()

        count_by_player_surface = {
            (row.player_id, row.surface): int(row.match_count)
            for row in surface_count_rows
        }
        peak_by_player_surface = {
            (row.player_id, row.surface): float(row.career_peak)
            for row in surface_peak_rows
        }

        for row in surface_recovery_rows:
            key = (row.player_id, row.surface)
            rating = float(row.elo_post)
            surface_states[key] = _SurfaceState(
                player_id=row.player_id,
                surface=row.surface,
                rating=rating,
                match_count=count_by_player_surface.get(key, 0),
                last_temporal_order=int(row.temporal_order),
                last_match_date=date_from_temporal_order(int(row.temporal_order)),
                career_peak=peak_by_player_surface.get(key, rating),
            )

    # ------------------------------------------------------------------
    # Core ELO computation (pure in-memory, no DB calls)
    # ------------------------------------------------------------------

    def _process_matches(
        self,
        matches: list[_MatchRow],
        states: dict[int, _PlayerState],
        surface_states: dict[tuple[int, str], _SurfaceState],
        progress_callback: Callable[[int, int], None] | None = None,
        progress_every: int = 0,
    ) -> tuple[list[_MatchUpdate], set[int], list[_SurfaceSnapshotUpdate], set[tuple[int, str]]]:
        """
        Apply ELO updates for all matches in temporal order.

        This is the hot path — zero DB calls. Mutates states in-place.

        Args:
            matches: Sorted by temporal_order ASC, id ASC.
            states: In-memory player state dict, updated as we go.

        Returns:
            (
                list of overall match updates,
                set of touched player IDs,
                list of surface snapshot updates,
                set of touched (player_id, surface) keys,
            )
        """
        params = self.params
        updates: list[_MatchUpdate] = []
        touched: set[int] = set()
        surface_updates: list[_SurfaceSnapshotUpdate] = []
        touched_surface_keys: set[tuple[int, str]] = set()

        total_matches = len(matches)
        for idx, match in enumerate(matches, start=1):
            pid_a = match.player_a_id
            pid_b = match.player_b_id

            # Corrupt source rows occasionally produce self-vs-self matches.
            # Skip these to avoid invalid Elo transitions and ON CONFLICT
            # duplicate-key updates in snapshot inserts.
            if pid_a == pid_b:
                if progress_callback and (
                    idx == total_matches
                    or (progress_every > 0 and idx % progress_every == 0)
                ):
                    progress_callback(idx, total_matches)
                continue

            # Initialise state for players we haven't seen yet.
            # This happens when player_ids filter was used and the opponent's
            # state wasn't pre-loaded — fall back to tour-appropriate default.
            if pid_a not in states:
                initial = initial_elo_for_level_code(params, match.level_code)
                states[pid_a] = _PlayerState(
                    player_id=pid_a, rating=initial, career_peak=initial
                )
            if pid_b not in states:
                initial = initial_elo_for_level_code(params, match.level_code)
                states[pid_b] = _PlayerState(
                    player_id=pid_b, rating=initial, career_peak=initial
                )

            state_a = states[pid_a]
            state_b = states[pid_b]
            match_date = match.match_date
            initial_rating = initial_elo_for_level_code(params, match.level_code)
            surface = match.surface
            key_a = (pid_a, surface)
            key_b = (pid_b, surface)

            if key_a not in surface_states:
                surface_states[key_a] = _SurfaceState(
                    player_id=pid_a,
                    surface=surface,
                    rating=initial_rating,
                    career_peak=initial_rating,
                )
            if key_b not in surface_states:
                surface_states[key_b] = _SurfaceState(
                    player_id=pid_b,
                    surface=surface,
                    rating=initial_rating,
                    career_peak=initial_rating,
                )
            surface_state_a = surface_states[key_a]
            surface_state_b = surface_states[key_b]

            # ---- Step 1: Inactivity decay ----
            # Ratings drift toward the tour baseline after 60+ days without a match.
            before_a = state_a.rating
            before_b = state_b.rating
            days_a: int | None = None
            days_b: int | None = None

            if state_a.last_match_date is not None and match_date is not None:
                days_a = (match_date - state_a.last_match_date).days
                before_a = apply_inactivity_decay(
                    before_a,
                    days_a,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                    target_rating=initial_rating,
                )
            if state_b.last_match_date is not None and match_date is not None:
                days_b = (match_date - state_b.last_match_date).days
                before_b = apply_inactivity_decay(
                    before_b,
                    days_b,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                    target_rating=initial_rating,
                )

            # ---- Step 2: K-boost for new / returning players ----
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

            # ---- Step 3: Margin-of-victory K multiplier ----
            winner = "A" if match.winner_id == pid_a else "B"
            margin_mult = 1.0
            if match.score_structured:
                margin_mult = float(
                    calculate_margin_multiplier(
                        match.score_structured,
                        winner,
                        margin_base=params.margin_base,
                        margin_scale=params.margin_scale,
                    ).multiplier
                )

            # ---- Step 4: ELO calculation ----
            base_k = params.get_k(match.level_code)
            s = params.get_s(match.level_code)
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

            # ---- Step 5: Update in-memory state ----
            state_a.rating = new_a
            state_b.rating = new_b
            state_a.match_count += 1
            state_b.match_count += 1
            state_a.last_temporal_order = match.temporal_order
            state_b.last_temporal_order = match.temporal_order
            if match_date is not None:
                state_a.last_match_date = match_date
                state_b.last_match_date = match_date
            state_a.career_peak = max(state_a.career_peak, new_a)
            state_b.career_peak = max(state_b.career_peak, new_b)

            touched.add(pid_a)
            touched.add(pid_b)

            # elo_pre stores the post-decay rating — the actual value used as
            # input to the ELO formula. This enables exact reproduction.
            updates.append(
                _MatchUpdate(
                    match_id=match.id,
                    elo_pre_player_a=round(before_a, 2),
                    elo_pre_player_b=round(before_b, 2),
                    elo_post_player_a=new_a,
                    elo_post_player_b=new_b,
                )
            )

            # ---- Step 6: Surface-specific ELO update ----
            s_before_a = surface_state_a.rating
            s_before_b = surface_state_b.rating
            s_days_a: int | None = None
            s_days_b: int | None = None

            if surface_state_a.last_match_date is not None and match_date is not None:
                s_days_a = (match_date - surface_state_a.last_match_date).days
                s_before_a = apply_inactivity_decay(
                    s_before_a,
                    s_days_a,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                    target_rating=initial_rating,
                )
            if surface_state_b.last_match_date is not None and match_date is not None:
                s_days_b = (match_date - surface_state_b.last_match_date).days
                s_before_b = apply_inactivity_decay(
                    s_before_b,
                    s_days_b,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                    target_rating=initial_rating,
                )

            s_boost_a = calculate_k_boost(
                surface_state_a.match_count,
                float(s_days_a) if s_days_a is not None else None,
                new_threshold=params.new_threshold,
                new_boost=params.new_boost,
                returning_days=params.returning_days,
                returning_boost=params.returning_boost,
            )
            s_boost_b = calculate_k_boost(
                surface_state_b.match_count,
                float(s_days_b) if s_days_b is not None else None,
                new_threshold=params.new_threshold,
                new_boost=params.new_boost,
                returning_days=params.returning_days,
                returning_boost=params.returning_boost,
            )

            s_new_a, s_new_b, _ = calculate_fast(
                s_before_a,
                s_before_b,
                winner,
                base_k * margin_mult * s_boost_a,
                base_k * margin_mult * s_boost_b,
                s,
            )
            s_new_a = round(s_new_a, 2)
            s_new_b = round(s_new_b, 2)

            surface_state_a.rating = s_new_a
            surface_state_b.rating = s_new_b
            surface_state_a.match_count += 1
            surface_state_b.match_count += 1
            surface_state_a.last_temporal_order = match.temporal_order
            surface_state_b.last_temporal_order = match.temporal_order
            if match_date is not None:
                surface_state_a.last_match_date = match_date
                surface_state_b.last_match_date = match_date
            surface_state_a.career_peak = max(surface_state_a.career_peak, s_new_a)
            surface_state_b.career_peak = max(surface_state_b.career_peak, s_new_b)

            touched_surface_keys.add(key_a)
            touched_surface_keys.add(key_b)
            surface_updates.append(
                _SurfaceSnapshotUpdate(
                    match_id=match.id,
                    player_id=pid_a,
                    surface=surface,
                    temporal_order=match.temporal_order,
                    elo_pre=round(s_before_a, 2),
                    elo_post=s_new_a,
                    elo_change=round(s_new_a - s_before_a, 2),
                )
            )
            surface_updates.append(
                _SurfaceSnapshotUpdate(
                    match_id=match.id,
                    player_id=pid_b,
                    surface=surface,
                    temporal_order=match.temporal_order,
                    elo_pre=round(s_before_b, 2),
                    elo_post=s_new_b,
                    elo_change=round(s_new_b - s_before_b, 2),
                )
            )
            if progress_callback and (
                idx == total_matches
                or (progress_every > 0 and idx % progress_every == 0)
            ):
                progress_callback(idx, total_matches)

        return updates, touched, surface_updates, touched_surface_keys

    # ------------------------------------------------------------------
    # Bulk DB writes
    # ------------------------------------------------------------------

    @staticmethod
    def _chunk_size_for_params(cols_per_row: int, max_params: int = 20000) -> int:
        """
        Compute a safe chunk size for INSERT ... VALUES (...) batches.

        Different drivers/backends can enforce lower bind-parameter caps
        (for example ~32767). Keep a conservative safety margin.
        """
        if cols_per_row <= 0:
            return 1
        return max(1, max_params // cols_per_row)

    def _bulk_write(
        self,
        session: Session,
        match_updates: list[_MatchUpdate],
        states: dict[int, _PlayerState],
        touched_ids: set[int],
        surface_states: dict[tuple[int, str], _SurfaceState],
        surface_snapshots: list[_SurfaceSnapshotUpdate],
        touched_surface_keys: set[tuple[int, str]],
    ) -> None:
        """
        Persist ELO results in bulk operations.

        1. executemany UPDATE on matches (SQLAlchemy uses execute_batch internally)
        2. INSERT ... ON CONFLICT DO UPDATE on player_elo_states
        3. INSERT ... ON CONFLICT DO UPDATE on player_surface_elo_states
        4. INSERT ... ON CONFLICT DO UPDATE on match_surface_elo_snapshots
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # -- Match ELO columns --
        match_rows = [
            {
                "id": u.match_id,
                "elo_pre_player_a": Decimal(str(u.elo_pre_player_a)),
                "elo_pre_player_b": Decimal(str(u.elo_pre_player_b)),
                "elo_post_player_a": Decimal(str(u.elo_post_player_a)),
                "elo_post_player_b": Decimal(str(u.elo_post_player_b)),
                "elo_params_version": self.params_version,
                "elo_processed_at": now,
                "elo_needs_recompute": False,
            }
            for u in match_updates
        ]
        session.execute(update(Match), match_rows)

        # -- Player ELO states (upsert: insert or update on player_id conflict) --
        state_rows = [
            {
                "player_id": pid,
                "rating": Decimal(str(round(states[pid].rating, 2))),
                "match_count": states[pid].match_count,
                "last_temporal_order": states[pid].last_temporal_order,
                "last_match_date": states[pid].last_match_date,
                "career_peak": Decimal(str(round(states[pid].career_peak, 2))),
                "updated_at": now,
            }
            for pid in touched_ids
        ]
        state_chunk_size = self._chunk_size_for_params(cols_per_row=7)
        for i in range(0, len(state_rows), state_chunk_size):
            chunk = state_rows[i : i + state_chunk_size]
            stmt = insert(PlayerEloState).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=[PlayerEloState.player_id],
                set_={
                    "rating": stmt.excluded.rating,
                    "match_count": stmt.excluded.match_count,
                    "last_temporal_order": stmt.excluded.last_temporal_order,
                    "last_match_date": stmt.excluded.last_match_date,
                    "career_peak": stmt.excluded.career_peak,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            session.execute(stmt)

        # -- Surface ELO states --
        if touched_surface_keys:
            surface_state_rows = [
                {
                    "player_id": pid,
                    "surface": surface,
                    "rating": Decimal(str(round(surface_states[(pid, surface)].rating, 2))),
                    "match_count": surface_states[(pid, surface)].match_count,
                    "last_temporal_order": surface_states[(pid, surface)].last_temporal_order,
                    "last_match_date": surface_states[(pid, surface)].last_match_date,
                    "career_peak": Decimal(str(round(surface_states[(pid, surface)].career_peak, 2))),
                    "updated_at": now,
                }
                for (pid, surface) in touched_surface_keys
            ]
            surface_state_chunk_size = self._chunk_size_for_params(cols_per_row=8)
            for i in range(0, len(surface_state_rows), surface_state_chunk_size):
                chunk = surface_state_rows[i : i + surface_state_chunk_size]
                surface_stmt = insert(PlayerSurfaceEloState).values(chunk)
                surface_stmt = surface_stmt.on_conflict_do_update(
                    index_elements=[PlayerSurfaceEloState.player_id, PlayerSurfaceEloState.surface],
                    set_={
                        "rating": surface_stmt.excluded.rating,
                        "match_count": surface_stmt.excluded.match_count,
                        "last_temporal_order": surface_stmt.excluded.last_temporal_order,
                        "last_match_date": surface_stmt.excluded.last_match_date,
                        "career_peak": surface_stmt.excluded.career_peak,
                        "updated_at": surface_stmt.excluded.updated_at,
                    },
                )
                session.execute(surface_stmt)

        # -- Per-match surface snapshots --
        if surface_snapshots:
            # Deduplicate keys within the write payload so ON CONFLICT does not
            # attempt to update the same target row multiple times in one stmt.
            deduped_snapshot_rows: dict[tuple[int, int, str], dict[str, object]] = {}
            for row in surface_snapshots:
                key = (row.match_id, row.player_id, row.surface)
                deduped_snapshot_rows[key] = {
                    "match_id": row.match_id,
                    "player_id": row.player_id,
                    "surface": row.surface,
                    "temporal_order": row.temporal_order,
                    "elo_pre": Decimal(str(row.elo_pre)),
                    "elo_post": Decimal(str(row.elo_post)),
                    "elo_change": Decimal(str(row.elo_change)),
                    "elo_params_version": self.params_version,
                    "processed_at": now,
                }

            snapshot_rows = list(deduped_snapshot_rows.values())
            snapshot_chunk_size = self._chunk_size_for_params(cols_per_row=9)
            for i in range(0, len(snapshot_rows), snapshot_chunk_size):
                chunk = snapshot_rows[i : i + snapshot_chunk_size]
                snapshot_stmt = insert(MatchSurfaceEloSnapshot).values(chunk)
                snapshot_stmt = snapshot_stmt.on_conflict_do_update(
                    index_elements=[
                        MatchSurfaceEloSnapshot.match_id,
                        MatchSurfaceEloSnapshot.player_id,
                        MatchSurfaceEloSnapshot.surface,
                    ],
                    set_={
                        "temporal_order": snapshot_stmt.excluded.temporal_order,
                        "elo_pre": snapshot_stmt.excluded.elo_pre,
                        "elo_post": snapshot_stmt.excluded.elo_post,
                        "elo_change": snapshot_stmt.excluded.elo_change,
                        "elo_params_version": snapshot_stmt.excluded.elo_params_version,
                        "processed_at": snapshot_stmt.excluded.processed_at,
                    },
                )
                session.execute(snapshot_stmt)

    def _refresh_pre_snapshots(
        self,
        session: Session,
        touched_player_ids: set[int],
    ) -> int:
        """
        Refresh elo_pre_player_a/b on upcoming/scheduled matches for touched players.

        After completing a processing batch, any future matches for those players
        should have their pre-match ELO snapshot updated so predictions stay accurate.

        Uses correlated subqueries against player_elo_states — one single UPDATE
        regardless of how many matches or players are involved.

        Returns the number of upcoming match rows updated.
        """
        if not touched_player_ids:
            return 0

        # Correlated subqueries pull the current rating per player
        rating_a_subq = (
            select(PlayerEloState.rating)
            .where(PlayerEloState.player_id == Match.player_a_id)
            .scalar_subquery()
        )
        rating_b_subq = (
            select(PlayerEloState.rating)
            .where(PlayerEloState.player_id == Match.player_b_id)
            .scalar_subquery()
        )

        stmt = (
            update(Match)
            .where(
                Match.status.in_(("upcoming", "scheduled")),
                Match.winner_id.is_(None),
                or_(
                    Match.player_a_id.in_(touched_player_ids),
                    Match.player_b_id.in_(touched_player_ids),
                ),
            )
            .values(
                elo_pre_player_a=rating_a_subq,
                elo_pre_player_b=rating_b_subq,
                elo_params_version=self.params_version,
            )
            .execution_options(synchronize_session=False)
        )
        result = session.execute(stmt)
        return result.rowcount
