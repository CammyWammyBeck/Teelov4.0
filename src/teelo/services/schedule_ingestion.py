"""
Schedule ingestion service — updates matches with schedule information.

This module processes ScrapedFixture objects from the daily schedule/order of
play page and updates existing Match rows with scheduling details:

- scheduled_date: The date the match is scheduled
- scheduled_datetime: Full datetime if time is available
- court: Which court the match is on
- status: Changes from 'upcoming' to 'scheduled'

The workflow assumes matches already exist in the database (created from draw
scraping with status='upcoming'). This service enriches those matches with
schedule data as it becomes available closer to match time, and updates the
status to 'scheduled'.

Match status lifecycle:
- Draw scraping: status = 'upcoming' (players known, no schedule yet)
- Schedule scraping: status = 'scheduled' (has date/time/court)
- Results scraping: status = 'completed' (or 'retired', 'walkover', etc.)

Usage:
    from teelo.services.schedule_ingestion import ingest_schedule

    # After scraping the daily schedule
    fixtures = await scraper.scrape_fixtures("australian-open")
    with get_session() as session:
        stats = ingest_schedule(session, fixtures, edition)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from teelo.db.models import Match, Player, TournamentEdition
from teelo.scrape.base import ScrapedFixture
from teelo.players.identity import PlayerIdentityService

logger = logging.getLogger(__name__)


def _make_external_id(
    year: int,
    tournament_id: str,
    round_code: str,
    player_a_ext_id: Optional[str],
    player_b_ext_id: Optional[str],
) -> Optional[str]:
    """
    Generate a match external_id compatible with the results/draw scrapers.

    Uses the same format as other scrapers: {year}_{tournament}_{round}_{sortedId1}_{sortedId2}
    This ensures schedule updates can find matches created by draw or results scraping.

    Args:
        year: Tournament year
        tournament_id: Tournament slug (e.g., "australian-open")
        round_code: Normalized round code (e.g., "R64", "QF")
        player_a_ext_id: Player A's ATP/WTA/ITF ID
        player_b_ext_id: Player B's ATP/WTA/ITF ID

    Returns:
        External ID string, or None if player IDs are missing
    """
    if not player_a_ext_id or not player_b_ext_id:
        return None

    sorted_ids = sorted([player_a_ext_id.upper(), player_b_ext_id.upper()])
    return f"{year}_{tournament_id}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"


@dataclass
class ScheduleIngestionStats:
    """Statistics from a schedule ingestion run."""
    total_fixtures: int = 0
    matches_updated: int = 0
    matches_not_found: int = 0
    skipped_no_player_ids: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        """Return a human-readable summary of ingestion results."""
        lines = [
            f"Schedule ingestion complete:",
            f"  Total fixtures processed: {self.total_fixtures}",
            f"  Matches updated:          {self.matches_updated}",
            f"  Matches not found:        {self.matches_not_found}",
            f"  Skipped (no player IDs):  {self.skipped_no_player_ids}",
        ]
        if self.errors:
            lines.append(f"  Errors: {len(self.errors)}")
            for err in self.errors[:5]:
                lines.append(f"    - {err}")
            if len(self.errors) > 5:
                lines.append(f"    ... and {len(self.errors) - 5} more")
        return "\n".join(lines)


def ingest_schedule(
    session: Session,
    fixtures: list[ScrapedFixture],
    edition: TournamentEdition,
    identity_service: Optional[PlayerIdentityService] = None,
) -> ScheduleIngestionStats:
    """
    Process a list of ScrapedFixture objects and update Match rows with schedule data.

    This function finds existing matches by external_id (generated from player IDs)
    and updates them with schedule information (date, time, court).

    Args:
        session: SQLAlchemy database session
        fixtures: List of ScrapedFixture from the schedule scraper
        edition: TournamentEdition these fixtures belong to

    Returns:
        ScheduleIngestionStats with counts of what happened
    """
    stats = ScheduleIngestionStats(total_fixtures=len(fixtures))

    for fixture in fixtures:
        try:
            # If external IDs are missing, try to resolve from DB using names
            if (not fixture.player_a_external_id or not fixture.player_b_external_id) and identity_service:
                _fill_missing_external_ids(session, fixture, identity_service)

            # Still missing: skip (can't generate external_id)
            if not fixture.player_a_external_id or not fixture.player_b_external_id:
                stats.skipped_no_player_ids += 1
                logger.debug(
                    "Skipping fixture without player IDs: %s vs %s",
                    fixture.player_a_name, fixture.player_b_name,
                )
                continue

            # Generate external_id to find the match
            external_id = _make_external_id(
                year=fixture.tournament_year,
                tournament_id=fixture.tournament_id,
                round_code=fixture.round,
                player_a_ext_id=fixture.player_a_external_id,
                player_b_ext_id=fixture.player_b_external_id,
            )

            if not external_id:
                stats.skipped_no_player_ids += 1
                continue

            # Find the existing match
            match = session.query(Match).filter(
                Match.external_id == external_id
            ).first()

            if not match:
                # Try finding by tournament + round + players (fallback)
                # This handles cases where external_id format might differ slightly
                match = _find_match_by_players(
                    session, edition, fixture
                )

            if not match:
                stats.matches_not_found += 1
                logger.debug(
                    "Match not found for %s vs %s (%s %s)",
                    fixture.player_a_name, fixture.player_b_name,
                    fixture.round, fixture.tournament_id,
                )
                continue

            # Update the match with schedule data
            updated = _update_match_schedule(match, fixture)

            if updated:
                stats.matches_updated += 1
                logger.info(
                    "Updated schedule for %s vs %s: %s %s on %s",
                    fixture.player_a_name, fixture.player_b_name,
                    fixture.scheduled_date, fixture.scheduled_time,
                    fixture.court,
                )

        except Exception as e:
            error_msg = f"{fixture.player_a_name} vs {fixture.player_b_name}: {e}"
            stats.errors.append(error_msg)
            logger.error("Error processing fixture: %s", error_msg)

    logger.info(stats.summary())
    return stats


def _find_match_by_players(
    session: Session,
    edition: TournamentEdition,
    fixture: ScrapedFixture,
) -> Optional[Match]:
    """
    Find a match by tournament, round, and player IDs (fallback lookup).

    Used when external_id lookup fails, which can happen if the match was
    created with a slightly different external_id format.

    Args:
        session: Database session
        edition: Tournament edition
        fixture: ScrapedFixture with player info

    Returns:
        Match if found, None otherwise
    """
    from teelo.db.models import Player

    # Find player IDs from external IDs (source-aware)
    player_a = _find_player_by_external_id(
        session, fixture.player_a_external_id, fixture.source
    )
    player_b = _find_player_by_external_id(
        session, fixture.player_b_external_id, fixture.source
    )

    if not player_a or not player_b:
        return None

    # Find match with these players in this tournament/round
    # Check both orderings (player_a/b could be swapped)
    match = session.query(Match).filter(
        Match.tournament_edition_id == edition.id,
        Match.round == fixture.round,
        (
            (Match.player_a_id == player_a.id) & (Match.player_b_id == player_b.id) |
            (Match.player_a_id == player_b.id) & (Match.player_b_id == player_a.id)
        )
    ).first()

    return match


def _find_player_by_external_id(
    session: Session,
    external_id: Optional[str],
    source: str,
) -> Optional[Player]:
    if not external_id:
        return None

    normalized_source = source.lower()
    if normalized_source in {"itf_men", "itf_women", "itf-men", "itf-women"}:
        normalized_source = "itf"

    ext_id = external_id.upper()
    if normalized_source == "atp":
        return session.query(Player).filter(Player.atp_id == ext_id).first()
    if normalized_source == "wta":
        return session.query(Player).filter(Player.wta_id == ext_id).first()
    if normalized_source == "itf":
        return session.query(Player).filter(Player.itf_id == ext_id).first()
    return None


def _fill_missing_external_ids(
    session: Session,
    fixture: ScrapedFixture,
    identity_service: PlayerIdentityService,
) -> None:
    if not fixture.player_a_external_id and fixture.player_a_name:
        match = identity_service.find_player(
            name=fixture.player_a_name, source=fixture.source
        )
        if match:
            player = session.query(Player).filter(Player.id == match.player_id).first()
            if player:
                fixture.player_a_external_id = _get_source_external_id(player, fixture.source)

    if not fixture.player_b_external_id and fixture.player_b_name:
        match = identity_service.find_player(
            name=fixture.player_b_name, source=fixture.source
        )
        if match:
            player = session.query(Player).filter(Player.id == match.player_id).first()
            if player:
                fixture.player_b_external_id = _get_source_external_id(player, fixture.source)


def _get_source_external_id(player: Player, source: str) -> Optional[str]:
    normalized_source = source.lower()
    if normalized_source in {"itf_men", "itf_women", "itf-men", "itf-women"}:
        normalized_source = "itf"

    if normalized_source == "atp":
        return player.atp_id
    if normalized_source == "wta":
        return player.wta_id
    if normalized_source == "itf":
        return player.itf_id
    return None


def _update_match_schedule(
    match: Match,
    fixture: ScrapedFixture,
) -> bool:
    """
    Update a match with schedule information from a fixture.

    Only updates fields that are provided in the fixture and don't already
    have values in the match (unless the fixture has more precise data).

    Also updates status from 'upcoming' to 'scheduled' when schedule data
    is added, indicating the match now appears on the order of play.

    Args:
        match: Existing Match to update
        fixture: ScrapedFixture with schedule data

    Returns:
        True if any fields were updated, False otherwise
    """
    updated = False

    # Parse scheduled_date
    if fixture.scheduled_date:
        try:
            scheduled_date = datetime.strptime(fixture.scheduled_date, "%Y-%m-%d").date()
            if match.scheduled_date != scheduled_date:
                match.scheduled_date = scheduled_date
                updated = True
        except ValueError:
            logger.warning("Invalid scheduled_date format: %s", fixture.scheduled_date)

    # Parse scheduled_datetime (combine date + time if both available)
    if fixture.scheduled_date and fixture.scheduled_time:
        try:
            datetime_str = f"{fixture.scheduled_date} {fixture.scheduled_time}"
            # Try common time formats
            for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %I:%M %p"]:
                try:
                    scheduled_datetime = datetime.strptime(datetime_str, fmt)
                    if match.scheduled_datetime != scheduled_datetime:
                        match.scheduled_datetime = scheduled_datetime
                        updated = True
                    break
                except ValueError:
                    continue
        except Exception as e:
            logger.warning("Could not parse scheduled_datetime: %s %s - %s",
                          fixture.scheduled_date, fixture.scheduled_time, e)

    # Update court
    if fixture.court and fixture.court != match.court:
        match.court = fixture.court
        updated = True

    # Update status from 'upcoming' to 'scheduled' when schedule data is added
    # This indicates the match now appears on the order of play
    if updated and match.status == "upcoming":
        match.status = "scheduled"
        logger.debug(
            "Match status changed from 'upcoming' to 'scheduled' for %s",
            match.external_id,
        )

    return updated


def ingest_single_fixture(
    session: Session,
    fixture: ScrapedFixture,
    edition: TournamentEdition,
) -> Optional[Match]:
    """
    Process a single fixture and update the corresponding match.

    Convenience function for updating one match at a time (e.g., from live updates).

    Args:
        session: Database session
        fixture: ScrapedFixture to process
        edition: Tournament edition

    Returns:
        Updated Match if found and updated, None otherwise
    """
    stats = ingest_schedule(session, [fixture], edition)

    if stats.matches_updated > 0:
        # Re-query to return the updated match
        external_id = _make_external_id(
            year=fixture.tournament_year,
            tournament_id=fixture.tournament_id,
            round_code=fixture.round,
            player_a_ext_id=fixture.player_a_external_id,
            player_b_ext_id=fixture.player_b_external_id,
        )
        if external_id:
            return session.query(Match).filter(Match.external_id == external_id).first()

    return None
