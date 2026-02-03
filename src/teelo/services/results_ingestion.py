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
from typing import Optional

from sqlalchemy.orm import Session

from teelo.db.models import (
    Match,
    TournamentEdition,
    estimate_match_date_from_round,
)
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.base import ScrapedMatch
from teelo.scrape.parsers.score import ScoreParseError, parse_score

logger = logging.getLogger(__name__)


@dataclass
class ResultsIngestionStats:
    """Statistics from a results ingestion run."""
    total_matches: int = 0
    matches_created: int = 0
    matches_updated: int = 0
    matches_skipped_duplicate: int = 0
    skipped_no_player_match: int = 0
    errors: list[str] = field(default_factory=list)

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

    # Track external_ids seen in this batch to avoid in-batch duplicates
    seen_external_ids: set[str] = set()

    for scraped in scraped_matches:
        try:
            result = _process_single_result(
                session=session,
                scraped=scraped,
                edition=edition,
                identity_service=identity_service,
                seen_external_ids=seen_external_ids,
                update_existing=update_existing,
            )

            if result == "created":
                stats.matches_created += 1
            elif result == "updated":
                stats.matches_updated += 1
            elif result == "duplicate":
                stats.matches_skipped_duplicate += 1
            elif result == "no_player":
                stats.skipped_no_player_match += 1

        except Exception as e:
            error_msg = f"{scraped.player_a_name} vs {scraped.player_b_name}: {e}"
            stats.errors.append(error_msg)
            logger.error("Error processing match: %s", error_msg)

    logger.info(stats.summary())
    return stats


def _process_single_result(
    session: Session,
    scraped: ScrapedMatch,
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
    seen_external_ids: set[str],
    update_existing: bool,
) -> str:
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
    # Check for in-batch duplicate
    if scraped.external_id in seen_external_ids:
        return "duplicate"
    seen_external_ids.add(scraped.external_id)

    # Resolve player A
    player_a_id = _resolve_player(
        session, scraped.player_a_name, scraped.player_a_external_id,
        scraped.source, scraped.player_a_nationality, identity_service,
    )
    if not player_a_id:
        logger.warning("Could not resolve player A: %s", scraped.player_a_name)
        return "no_player"

    # Resolve player B
    player_b_id = _resolve_player(
        session, scraped.player_b_name, scraped.player_b_external_id,
        scraped.source, scraped.player_b_nationality, identity_service,
    )
    if not player_b_id:
        logger.warning("Could not resolve player B: %s", scraped.player_b_name)
        return "no_player"

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
    existing = session.query(Match).filter(
        Match.external_id == scraped.external_id
    ).first()

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
            return "updated"
        else:
            return "duplicate"

    # Create new match
    match = Match(
        external_id=scraped.external_id,
        source=scraped.source,
        tournament_edition_id=edition.id,
        round=scraped.round,
        match_number=scraped.match_number,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        player_a_seed=scraped.player_a_seed,
        player_b_seed=scraped.player_b_seed,
        winner_id=player_a_id,  # Player A is typically the winner in results
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
    return "created"


def _resolve_player(
    session: Session,
    name: str,
    external_id: Optional[str],
    source: str,
    nationality: Optional[str],
    identity_service: PlayerIdentityService,
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
    player_id, _ = identity_service.find_or_queue_player(
        name=name,
        source=source,
        external_id=external_id,
    )

    # Fallback: create player if we have an external ID
    if not player_id and external_id:
        player_id = identity_service.create_player(
            name=name,
            source=source,
            external_id=external_id,
            nationality=nationality,
        )

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
    match.winner_id = player_a_id  # Player A is winner in results
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
