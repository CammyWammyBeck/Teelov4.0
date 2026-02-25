"""
Results ingestion service — processes match results into the database.

This module handles updating matches with results after they complete:

- score: Raw score string (e.g., "6-4 3-6 7-6(5)")
- score_structured: Parsed JSON representation
- winner_id: Foreign key to winning player
- match_date: Actual date the match was played
- duration_minutes: Match duration
- status: Final status (completed, retired, walkover, default)

The workflow assumes matches may already exist in the database (created from
draw scraping). This service updates those matches with results, or creates
new matches if they don't exist (for historical backfill).

Key design decision: By default, this service UPDATES existing matches rather
than skipping them. This ensures the pipeline flows correctly:
    Draw → Schedule → Results

Usage:
    from teelo.services.results_ingestion import ingest_results

    # After scraping results
    async for match in scraper.scrape_tournament_results("australian-open", 2025):
        matches.append(match)

    with get_session() as session:
        stats = ingest_results(session, matches, edition, identity_service)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from itertools import islice
from typing import Optional

from sqlalchemy.orm import Session

from teelo.db.models import (
    Match,
    Player,
    TournamentEdition,
    estimate_match_date_from_round,
)
from teelo.players.aliases import normalize_name
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.base import ScrapedMatch
from teelo.scrape.parsers.score import ScoreParseError, parse_score

logger = logging.getLogger(__name__)
PlayerResolutionKey = tuple[str, str, Optional[str], Optional[str]]
MatchPairKey = tuple[int, str, int, int]


def _determine_winner_id(
    scraped: ScrapedMatch,
    player_a_id: int,
    player_b_id: int,
) -> Optional[int]:
    """
    Determine winner_id by comparing scraped winner_name against both player names.

    Returns None if no winner_name is set (e.g. upcoming match).
    Falls back to player_a_id with a warning if winner_name doesn't match either.
    """
    if not scraped.winner_name:
        return None
    if scraped.winner_name == scraped.player_a_name:
        return player_a_id
    if scraped.winner_name == scraped.player_b_name:
        return player_b_id
    # Fallback: winner_name doesn't match either player name exactly.
    # This shouldn't happen, but log and default to player_a for safety.
    logger.warning(
        "Winner name '%s' doesn't match player_a '%s' or player_b '%s'; "
        "defaulting to player_a as winner",
        scraped.winner_name, scraped.player_a_name, scraped.player_b_name,
    )
    return player_a_id


@dataclass
class ResultsIngestionStats:
    """Statistics from a results ingestion run."""
    total_matches: int = 0
    matches_created: int = 0
    matches_updated: int = 0
    matches_skipped_duplicate: int = 0
    skipped_no_player_match: int = 0
    errors: list[str] = field(default_factory=list)
    # IDs of terminal matches (created or updated with a result) in this run.
    # Populated after each batch flush so IDs are guaranteed to be set.
    # Used by the pipeline for inline ELO updates without a re-query.
    completed_match_ids: list[int] = field(default_factory=list)

    def summary(self) -> str:
        """Return a human-readable summary of ingestion results."""
        lines = [
            f"Results ingestion complete:",
            f"  Total matches processed:  {self.total_matches}",
            f"  Matches created:          {self.matches_created}",
            f"  Matches updated:          {self.matches_updated}",
            f"  Skipped (in-batch dup):   {self.matches_skipped_duplicate}",
            f"  Skipped (no player ID):   {self.skipped_no_player_match}",
        ]
        if self.errors:
            lines.append(f"  Errors: {len(self.errors)}")
            for err in self.errors[:5]:
                lines.append(f"    - {err}")
            if len(self.errors) > 5:
                lines.append(f"    ... and {len(self.errors) - 5} more")
        return "\n".join(lines)


def ingest_results(
    session: Session,
    scraped_matches: list[ScrapedMatch],
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
    update_existing: bool = True,
) -> ResultsIngestionStats:
    """
    Process a list of ScrapedMatch objects into Match rows.

    This is the main entry point for results data. It:
    1. Resolves player IDs using the identity service
    2. Finds existing matches by external_id
    3. Updates existing matches with results (if update_existing=True)
    4. Creates new matches if none exist

    Args:
        session: SQLAlchemy database session
        scraped_matches: List of ScrapedMatch from the results scraper
        edition: TournamentEdition these matches belong to
        identity_service: Service for resolving player names → canonical IDs
        update_existing: If True (default), update existing matches with results.
                        If False, skip matches that already exist.

    Returns:
        ResultsIngestionStats with counts of what happened
    """
    stats = ResultsIngestionStats(total_matches=len(scraped_matches))
    player_resolution_cache = _preload_player_resolutions(scraped_matches, identity_service)
    existing_by_external_id, existing_by_pair = _preload_existing_match_indexes(session, edition.id)
    player_external_id_cache: dict[tuple[int, str], Optional[str]] = {}

    # Track external_ids seen in this batch to avoid in-batch duplicates
    seen_external_ids: set[str] = set()

    batch_size = 100
    for batch in _chunked(scraped_matches, batch_size):
        _process_results_batch(
            session=session,
            batch=batch,
            stats=stats,
            edition=edition,
            identity_service=identity_service,
            player_resolution_cache=player_resolution_cache,
            existing_by_external_id=existing_by_external_id,
            existing_by_pair=existing_by_pair,
            player_external_id_cache=player_external_id_cache,
            seen_external_ids=seen_external_ids,
            update_existing=update_existing,
        )

    logger.info(stats.summary())
    return stats


def _chunked(items: list[ScrapedMatch], size: int) -> list[list[ScrapedMatch]]:
    """Split a list into fixed-size chunks."""
    if size <= 0:
        return [items]
    iterator = iter(items)
    chunks: list[list[ScrapedMatch]] = []
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return chunks
        chunks.append(chunk)


def _increment_stats_for_result(stats: ResultsIngestionStats, result: str) -> None:
    if result == "created":
        stats.matches_created += 1
    elif result == "updated":
        stats.matches_updated += 1
    elif result == "duplicate":
        stats.matches_skipped_duplicate += 1
    elif result == "no_player":
        stats.skipped_no_player_match += 1


def _process_results_batch(
    session: Session,
    batch: list[ScrapedMatch],
    stats: ResultsIngestionStats,
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
    player_resolution_cache: dict[PlayerResolutionKey, Optional[int]],
    existing_by_external_id: dict[str, Match],
    existing_by_pair: dict[MatchPairKey, Match],
    player_external_id_cache: dict[tuple[int, str], Optional[str]],
    seen_external_ids: set[str],
    update_existing: bool,
) -> None:
    # Run the common path in one savepoint + flush to avoid per-row savepoint
    # overhead. If it fails, fallback to row-level isolation for this batch.
    batch_external = dict(existing_by_external_id)
    batch_pairs = dict(existing_by_pair)
    batch_seen = set(seen_external_ids)
    batch_results: list[tuple[str, Optional[Match]]] = []
    try:
        with session.begin_nested():
            for scraped in batch:
                result, match = _process_single_result(
                    session=session,
                    scraped=scraped,
                    edition=edition,
                    identity_service=identity_service,
                    player_resolution_cache=player_resolution_cache,
                    existing_by_external_id=batch_external,
                    existing_by_pair=batch_pairs,
                    player_external_id_cache=player_external_id_cache,
                    seen_external_ids=batch_seen,
                    update_existing=update_existing,
                )
                batch_results.append((result, match))
            # flush assigns DB IDs to newly created Match rows
            session.flush()

        existing_by_external_id.clear()
        existing_by_external_id.update(batch_external)
        existing_by_pair.clear()
        existing_by_pair.update(batch_pairs)
        seen_external_ids.clear()
        seen_external_ids.update(batch_seen)
        for result, match in batch_results:
            _increment_stats_for_result(stats, result)
            # Collect IDs of terminal matches so the pipeline can run ELO inline
            # without a re-query. IDs are valid because flush() ran above.
            if result in ("created", "updated") and match is not None and match.id and match.winner_id:
                stats.completed_match_ids.append(match.id)
        return
    except Exception as batch_error:
        logger.warning(
            "Results batch failed; retrying row-by-row for %d matches: %s",
            len(batch),
            batch_error,
        )

    for scraped in batch:
        try:
            with session.begin_nested():
                result, match = _process_single_result(
                    session=session,
                    scraped=scraped,
                    edition=edition,
                    identity_service=identity_service,
                    player_resolution_cache=player_resolution_cache,
                    existing_by_external_id=existing_by_external_id,
                    existing_by_pair=existing_by_pair,
                    player_external_id_cache=player_external_id_cache,
                    seen_external_ids=seen_external_ids,
                    update_existing=update_existing,
                )
                session.flush()
            _increment_stats_for_result(stats, result)
            if result in ("created", "updated") and match is not None and match.id and match.winner_id:
                stats.completed_match_ids.append(match.id)
        except Exception as e:
            error_msg = f"{scraped.player_a_name} vs {scraped.player_b_name}: {e}"
            stats.errors.append(error_msg)
            logger.error("Error processing match: %s", error_msg)


def _process_single_result(
    session: Session,
    scraped: ScrapedMatch,
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
    player_resolution_cache: dict[PlayerResolutionKey, Optional[int]],
    existing_by_external_id: dict[str, Match],
    existing_by_pair: dict[MatchPairKey, Match],
    player_external_id_cache: dict[tuple[int, str], Optional[str]],
    seen_external_ids: set[str],
    update_existing: bool,
) -> tuple[str, Optional[Match]]:
    """
    Process a single scraped match result.

    Args:
        session: Database session
        scraped: ScrapedMatch to process
        edition: Tournament edition
        identity_service: Player identity service
        seen_external_ids: Set of external_ids already seen in this batch
        update_existing: Whether to update existing matches

    Returns:
        Status string: "created", "updated", "duplicate", "no_player", or "skipped"
    """
    # Resolve external IDs from DB if missing (only save if we have player IDs)
    player_a_id = _resolve_player(
        session, scraped.player_a_name, scraped.player_a_external_id,
        scraped.source, scraped.player_a_nationality, identity_service, player_resolution_cache,
    )
    if not player_a_id:
        logger.warning("Could not resolve player A: %s", scraped.player_a_name)
        return "no_player", None

    player_b_id = _resolve_player(
        session, scraped.player_b_name, scraped.player_b_external_id,
        scraped.source, scraped.player_b_nationality, identity_service, player_resolution_cache,
    )
    if not player_b_id:
        logger.warning("Could not resolve player B: %s", scraped.player_b_name)
        return "no_player", None

    external_id = _make_external_id_from_players(
        session=session,
        year=scraped.tournament_year,
        tournament_id=scraped.tournament_id,
        round_code=scraped.round,
        source=scraped.source,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        scraped_a_ext=scraped.player_a_external_id,
        scraped_b_ext=scraped.player_b_external_id,
        player_external_id_cache=player_external_id_cache,
    )

    # Check for in-batch duplicate
    dedupe_key = _make_dedupe_key(
        edition_id=edition.id,
        round_code=scraped.round,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        match_date=scraped.match_date,
        external_id=external_id,
    )
    if dedupe_key in seen_external_ids:
        return "duplicate", None
    seen_external_ids.add(dedupe_key)

    # Parse score
    score_structured = None
    if scraped.score_raw:
        try:
            parsed = parse_score(scraped.score_raw)
            score_structured = parsed.to_structured()
        except ScoreParseError:
            pass

    # Parse match date
    match_date = None
    match_date_estimated = False
    if scraped.match_date:
        try:
            match_date = datetime.strptime(scraped.match_date, "%Y-%m-%d").date()
        except ValueError:
            pass

    # Estimate date if not provided
    if match_date is None and edition.start_date and edition.end_date:
        match_date = estimate_match_date_from_round(
            round_code=scraped.round or "R128",
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )
        if match_date is not None:
            match_date_estimated = True

    # Check for existing match
    existing: Optional[Match] = None
    if external_id:
        existing = existing_by_external_id.get(external_id)

    if not existing:
        pair_key = _make_pair_match_key(
            edition=edition,
            round_code=scraped.round,
            player_a_id=player_a_id,
            player_b_id=player_b_id,
        )
        existing = existing_by_pair.get(pair_key)

    if existing:
        if update_existing:
            _update_match_with_result(
                match=existing,
                scraped=scraped,
                edition=edition,
                player_a_id=player_a_id,
                player_b_id=player_b_id,
                score_structured=score_structured,
                match_date=match_date,
                match_date_estimated=match_date_estimated,
            )
            if external_id and (not existing.external_id or existing.external_id.startswith("draw_")):
                if existing.external_id:
                    existing_by_external_id.pop(existing.external_id, None)
                existing.external_id = external_id
                existing_by_external_id[external_id] = existing
            return "updated", existing
        else:
            return "duplicate", None

    # Create new match
    match = Match(
        external_id=external_id,
        source=scraped.source,
        tournament_edition_id=edition.id,
        round=scraped.round,
        match_number=scraped.match_number,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        player_a_seed=scraped.player_a_seed,
        player_b_seed=scraped.player_b_seed,
        winner_id=_determine_winner_id(scraped, player_a_id, player_b_id),
        score=scraped.score_raw,
        score_structured=score_structured,
        match_date=match_date,
        match_date_estimated=match_date_estimated,
        duration_minutes=scraped.duration_minutes,
        status=scraped.status,
        retirement_set=scraped.retirement_set,
    )

    # Compute temporal order
    match.update_temporal_order(
        tournament_start=edition.start_date,
        tournament_end=edition.end_date,
    )

    session.add(match)
    existing_by_pair[_make_pair_match_key(edition, scraped.round, player_a_id, player_b_id)] = match
    if external_id:
        existing_by_external_id[external_id] = match
    return "created", match


def _preload_player_resolutions(
    scraped_matches: list[ScrapedMatch],
    identity_service: PlayerIdentityService,
) -> dict[PlayerResolutionKey, Optional[int]]:
    """
    Resolve each unique player tuple once per tournament ingestion run.
    """
    cache: dict[PlayerResolutionKey, Optional[int]] = {}
    # Use a separate set for deduplication — do NOT pre-populate the cache
    # with None, because _resolve_player() short-circuits on any existing
    # cache entry and would return the pre-populated None without resolving.
    seen_keys: set[PlayerResolutionKey] = set()
    unique_players: list[PlayerResolutionKey] = []

    for scraped in scraped_matches:
        key_a: PlayerResolutionKey = (
            scraped.player_a_name,
            scraped.source,
            scraped.player_a_external_id,
            scraped.player_a_nationality,
        )
        key_b: PlayerResolutionKey = (
            scraped.player_b_name,
            scraped.source,
            scraped.player_b_external_id,
            scraped.player_b_nationality,
        )
        if key_a not in seen_keys:
            seen_keys.add(key_a)
            unique_players.append(key_a)
        if key_b not in seen_keys:
            seen_keys.add(key_b)
            unique_players.append(key_b)

    for name, source, external_id, nationality in unique_players:
        _resolve_player(
            session=identity_service.db,
            name=name,
            external_id=external_id,
            source=source,
            nationality=nationality,
            identity_service=identity_service,
            resolution_cache=cache,
        )

    return cache


def _preload_existing_match_indexes(
    session: Session,
    edition_id: int,
) -> tuple[dict[str, Match], dict[MatchPairKey, Match]]:
    """
    Load tournament matches once and index by both external_id and round+player pair.
    """
    matches = session.query(Match).filter(Match.tournament_edition_id == edition_id).all()
    by_external_id: dict[str, Match] = {}
    by_pair: dict[MatchPairKey, Match] = {}

    for match in matches:
        if match.external_id:
            by_external_id[match.external_id] = match
        if match.player_a_id and match.player_b_id:
            key = _make_pair_match_key_from_values(
                edition_id=edition_id,
                round_code=match.round,
                player_a_id=match.player_a_id,
                player_b_id=match.player_b_id,
            )
            by_pair.setdefault(key, match)

    return by_external_id, by_pair


def _resolve_player(
    session: Session,
    name: str,
    external_id: Optional[str],
    source: str,
    nationality: Optional[str],
    identity_service: PlayerIdentityService,
    resolution_cache: Optional[dict[PlayerResolutionKey, Optional[int]]] = None,
) -> Optional[int]:
    """
    Resolve a player name/ID to a canonical player_id.

    Args:
        session: Database session
        name: Player display name
        external_id: Tour-specific ID (ATP ID, etc.)
        source: Data source ('atp', 'wta', 'itf')
        nationality: Player nationality (IOC code)
        identity_service: Player matching service

    Returns:
        Canonical player_id or None if unresolvable
    """
    key: Optional[PlayerResolutionKey] = None
    if resolution_cache is not None:
        key = (name, source, external_id, nationality)
        if key in resolution_cache:
            return resolution_cache[key]

    player_id, _ = identity_service.find_or_queue_player(
        name=name,
        source=source,
        external_id=external_id,
    )

    # Fallback safety: one more abbreviated-name check before creating.
    if not player_id and external_id:
        abbreviated_match = identity_service._find_by_abbreviated_match(
            normalize_name(name)
        )
        if abbreviated_match:
            player_id = abbreviated_match.id

    # Fallback: create player if we have an external ID
    if not player_id and external_id:
        player_id = identity_service.create_player(
            name=name,
            source=source,
            external_id=external_id,
            nationality=nationality,
        )

    if resolution_cache is not None and key is not None:
        resolution_cache[key] = player_id
    return player_id


def _update_match_with_result(
    match: Match,
    scraped: ScrapedMatch,
    edition: TournamentEdition,
    player_a_id: int,
    player_b_id: int,
    score_structured: Optional[dict],
    match_date,
    match_date_estimated: bool,
) -> None:
    """
    Update an existing match with result data.

    This handles the transition from upcoming/scheduled → completed (or retired,
    walkover, etc.), preserving any schedule data (scheduled_date, court) that
    was already set.

    Args:
        match: Existing Match to update
        scraped: ScrapedMatch with result data
        edition: Tournament edition
        player_a_id: Resolved player A ID
        player_b_id: Resolved player B ID
        score_structured: Parsed score structure
        match_date: Actual match date
        match_date_estimated: Whether the date was estimated
    """
    # Update player IDs (should be the same, but ensure consistency)
    match.player_a_id = player_a_id
    match.player_b_id = player_b_id

    # Update seeds if provided by results scraper
    if scraped.player_a_seed is not None:
        match.player_a_seed = scraped.player_a_seed
    if scraped.player_b_seed is not None:
        match.player_b_seed = scraped.player_b_seed

    # Set result fields
    match.winner_id = _determine_winner_id(scraped, player_a_id, player_b_id)
    match.score = scraped.score_raw
    match.score_structured = score_structured
    match.status = scraped.status
    match.retirement_set = scraped.retirement_set
    match.duration_minutes = scraped.duration_minutes

    # Update match date (actual date takes precedence over estimated)
    # But don't overwrite with an estimated date if we already have a real one
    if match_date is not None:
        if not match_date_estimated or match.match_date is None:
            match.match_date = match_date
            match.match_date_estimated = match_date_estimated

    # Update round and match number if provided
    if scraped.round:
        match.round = scraped.round
    if scraped.match_number is not None:
        match.match_number = scraped.match_number

    # Recompute temporal order
    match.update_temporal_order(
        tournament_start=edition.start_date,
        tournament_end=edition.end_date,
    )


def _get_player_external_id(
    session: Session,
    player_id: int,
    source: str,
    player_external_id_cache: Optional[dict[tuple[int, str], Optional[str]]] = None,
) -> Optional[str]:
    normalized_source = source.lower()
    if normalized_source in {"itf_men", "itf_women", "itf-men", "itf-women"}:
        normalized_source = "itf"

    if player_external_id_cache is not None:
        cache_key = (player_id, normalized_source)
        if cache_key in player_external_id_cache:
            return player_external_id_cache[cache_key]

    player = session.query(Player).filter(Player.id == player_id).first()
    if not player:
        if player_external_id_cache is not None:
            player_external_id_cache[(player_id, normalized_source)] = None
        return None

    result: Optional[str]
    if normalized_source == "atp":
        result = player.atp_id
    elif normalized_source == "wta":
        result = player.wta_id
    elif normalized_source == "itf":
        result = player.itf_id
    else:
        result = None

    if player_external_id_cache is not None:
        player_external_id_cache[(player_id, normalized_source)] = result
    return result


def _make_external_id_from_players(
    session: Session,
    year: int,
    tournament_id: str,
    round_code: str,
    source: str,
    player_a_id: int,
    player_b_id: int,
    scraped_a_ext: Optional[str],
    scraped_b_ext: Optional[str],
    player_external_id_cache: Optional[dict[tuple[int, str], Optional[str]]] = None,
) -> Optional[str]:
    ext_a = scraped_a_ext or _get_player_external_id(
        session,
        player_a_id,
        source,
        player_external_id_cache=player_external_id_cache,
    )
    ext_b = scraped_b_ext or _get_player_external_id(
        session,
        player_b_id,
        source,
        player_external_id_cache=player_external_id_cache,
    )

    if not ext_a or not ext_b:
        return None

    sorted_ids = sorted([ext_a, ext_b])
    return f"{year}_{tournament_id}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"


def _make_dedupe_key(
    edition_id: int,
    round_code: str,
    player_a_id: int,
    player_b_id: int,
    match_date: Optional[str],
    external_id: Optional[str],
) -> str:
    if external_id:
        return f"external:{external_id}"

    a_id, b_id = sorted([player_a_id, player_b_id])
    date_part = match_date or ""
    return f"fallback:{edition_id}:{round_code}:{a_id}:{b_id}:{date_part}"


def _make_pair_match_key(
    edition: TournamentEdition,
    round_code: Optional[str],
    player_a_id: int,
    player_b_id: int,
) -> MatchPairKey:
    return _make_pair_match_key_from_values(
        edition_id=edition.id,
        round_code=round_code,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
    )


def _make_pair_match_key_from_values(
    edition_id: int,
    round_code: Optional[str],
    player_a_id: int,
    player_b_id: int,
) -> MatchPairKey:
    a_id, b_id = sorted([player_a_id, player_b_id])
    return edition_id, (round_code or ""), a_id, b_id


def ingest_single_result(
    session: Session,
    scraped: ScrapedMatch,
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
) -> Optional[Match]:
    """
    Process a single match result.

    Convenience function for updating one match at a time (e.g., from live updates).

    Args:
        session: Database session
        scraped: ScrapedMatch to process
        edition: Tournament edition
        identity_service: Player identity service

    Returns:
        Match if created/updated, None otherwise
    """
    stats = ingest_results(session, [scraped], edition, identity_service)

    if stats.matches_created > 0 or stats.matches_updated > 0:
        return session.query(Match).filter(
            Match.external_id == scraped.external_id
        ).first()

    return None
