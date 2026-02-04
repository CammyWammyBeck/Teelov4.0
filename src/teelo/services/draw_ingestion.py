"""
Draw ingestion service — processes scraped draw entries into the database.

This module handles the full lifecycle of tournament draw data:

1. **Ingestion** (ingest_draw): Takes ScrapedDrawEntry objects from the draw
   scraper and creates/updates Match rows with draw_position set. Byes are
   skipped (no match created). Completed matches get status='completed'.
   Matches with both players known get status='upcoming' (they move to
   'scheduled' when order of play is scraped). Entries with only one known
   player are skipped — those matches will be created later via propagation.

2. **Propagation** (propagate_draw_result): When a match with a draw_position
   completes, checks if the other feeder match in the same round also completed.
   If both feeders are done, creates the next-round match with both winners.
   This keeps player_a_id/player_b_id non-nullable.

3. **Bye processing** (process_byes): For draw entries marked as byes, finds
   the corresponding next-round slot and records that the bye player advances.
   This is done as part of ingestion so propagation can find feeder results.

The key design decision: matches are only created when both players are known.
Draw positions + bracket math replace the need for NULL player FKs or a
next_match_id foreign key.

Usage:
    from teelo.services.draw_ingestion import ingest_draw, propagate_draw_result

    # After scraping a draw
    entries = await scraper.scrape_tournament_draw("australian-open", 2025)
    with get_session() as session:
        stats = ingest_draw(session, entries, edition, identity_service)

    # After a match completes (e.g., from live score updates)
    with get_session() as session:
        propagate_draw_result(session, completed_match)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from teelo.db.models import (
    Match,
    TournamentEdition,
    compute_temporal_order,
    estimate_match_date_from_round,
)
from teelo.draw import (
    get_feeder_positions,
    get_next_draw_position,
    get_next_round,
    get_previous_round,
)
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.base import ScrapedDrawEntry
from teelo.scrape.parsers.score import ScoreParseError, parse_score

logger = logging.getLogger(__name__)


def _make_external_id(
    year: int,
    tournament_id: str,
    round_code: str,
    draw_position: int,
    player_a_ext_id: Optional[str] = None,
    player_b_ext_id: Optional[str] = None,
) -> Optional[str]:
    """
    Generate a match external_id compatible with the results scraper.

    When both player ATP IDs are known, uses the same format as the results
    scraper: {year}_{tournament}_{round}_{sortedId1}_{sortedId2}.
    This ensures draw-ingested matches won't be duplicated if the results
    scraper later processes the same tournament.

    Falls back to positional format (draw_{year}_{tournament}_{round}_{position})
    when player IDs aren't available (e.g., propagated matches before players
    are resolved).
    """
    if player_a_ext_id and player_b_ext_id:
        sorted_ids = sorted([player_a_ext_id, player_b_ext_id])
        return f"{year}_{tournament_id}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"
    return None


@dataclass
class DrawIngestionStats:
    """Statistics from a draw ingestion run."""
    total_entries: int = 0
    matches_created: int = 0
    matches_updated: int = 0
    matches_skipped_existing: int = 0
    byes_processed: int = 0
    skipped_tbd: int = 0          # Entries with missing player(s)
    skipped_no_player_match: int = 0  # Players couldn't be matched/created
    propagations_created: int = 0  # Next-round matches created from completed feeders
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        """Return a human-readable summary of ingestion results."""
        lines = [
            f"Draw ingestion complete:",
            f"  Total entries processed: {self.total_entries}",
            f"  Matches created:         {self.matches_created}",
            f"  Matches updated:         {self.matches_updated}",
            f"  Skipped (existing):      {self.matches_skipped_existing}",
            f"  Byes processed:          {self.byes_processed}",
            f"  Skipped (TBD players):   {self.skipped_tbd}",
            f"  Skipped (no ID match):   {self.skipped_no_player_match}",
            f"  Propagations created:    {self.propagations_created}",
        ]
        if self.errors:
            lines.append(f"  Errors: {len(self.errors)}")
            for err in self.errors[:5]:
                lines.append(f"    - {err}")
            if len(self.errors) > 5:
                lines.append(f"    ... and {len(self.errors) - 5} more")
        return "\n".join(lines)


# =============================================================================
# Main ingestion function
# =============================================================================

def ingest_draw(
    session: Session,
    entries: list[ScrapedDrawEntry],
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
    overwrite: bool = False,
) -> DrawIngestionStats:
    """
    Process a list of ScrapedDrawEntry objects into Match rows.

    This is the main entry point for draw data. It:
    1. Iterates all draw entries
    2. Skips byes (records them for propagation tracking)
    3. Skips entries where one or both players are TBD
    4. For entries with both players: resolves player IDs, creates/updates Match
    5. After all entries are processed, propagates completed results to create
       next-round matches where both feeders have completed

    Args:
        session: SQLAlchemy database session
        entries: List of ScrapedDrawEntry from the draw scraper
        edition: TournamentEdition these entries belong to
        identity_service: Service for resolving player names → canonical IDs
        overwrite: If True, update existing matches with fresh data

    Returns:
        DrawIngestionStats with counts of what happened
    """
    stats = DrawIngestionStats(total_entries=len(entries))

    # Track which draw positions have completed results (for propagation)
    # Key: (round, draw_position), Value: winner player_id
    completed_positions: dict[tuple[str, int], int] = {}

    # Track bye positions — the player who advances without playing
    # Key: (round, draw_position), Value: player_id who got the bye
    bye_positions: dict[tuple[str, int], int] = {}

    # Track external_ids seen in this batch to avoid duplicates in-session
    seen_external_ids: set[str] = set()

    # -------------------------------------------------------------------------
    # Phase 1: Process each draw entry
    # -------------------------------------------------------------------------
    for entry in entries:
        try:
            if entry.is_bye:
                # Resolve the bye player so we can track their advancement
                bye_player_id = _resolve_bye_player(
                    session, entry, identity_service
                )
                if bye_player_id:
                    bye_positions[(entry.round, entry.draw_position)] = bye_player_id
                    stats.byes_processed += 1
                else:
                    stats.skipped_no_player_match += 1
                continue

            # Skip entries with missing players (TBD / qualifier placeholders)
            if not entry.player_a_name or not entry.player_b_name:
                stats.skipped_tbd += 1
                continue

            # Resolve player IDs
            player_a_id = _resolve_player(
                session, entry.player_a_name, entry.player_a_external_id,
                entry.source, identity_service,
            )
            player_b_id = _resolve_player(
                session, entry.player_b_name, entry.player_b_external_id,
                entry.source, identity_service,
            )

            if not player_a_id or not player_b_id:
                stats.skipped_no_player_match += 1
                logger.warning(
                    "Could not resolve players for %s #%d: %s vs %s",
                    entry.round, entry.draw_position,
                    entry.player_a_name, entry.player_b_name,
                )
                continue

            # Create or update the match
            match = _upsert_draw_match(
                session,
                entry,
                edition,
                player_a_id,
                player_b_id,
                overwrite,
                seen_external_ids,
            )

            if match is None:
                stats.matches_skipped_existing += 1
            elif match.id is None:
                # New match (not yet flushed)
                stats.matches_created += 1
            else:
                stats.matches_updated += 1

            # Track completed positions for propagation
            if match and match.is_completed and match.winner_id:
                completed_positions[
                    (entry.round, entry.draw_position)
                ] = match.winner_id

        except Exception as e:
            error_msg = f"{entry.round} #{entry.draw_position}: {e}"
            stats.errors.append(error_msg)
            logger.error("Error processing draw entry: %s", error_msg)

    # Flush to get IDs for newly created matches
    session.flush()

    # -------------------------------------------------------------------------
    # Phase 2: Propagate results — create next-round matches
    # -------------------------------------------------------------------------
    # Merge completed positions with bye positions (byes count as "completed")
    all_decided: dict[tuple[str, int], int] = {}
    all_decided.update(bye_positions)
    all_decided.update(completed_positions)

    # Also load any previously completed matches for this edition that have
    # draw_position set (from prior ingestion runs)
    existing_completed = session.query(Match).filter(
        Match.tournament_edition_id == edition.id,
        Match.draw_position.isnot(None),
        Match.status.in_(["completed", "retired", "walkover", "default"]),
        Match.winner_id.isnot(None),
    ).all()

    for m in existing_completed:
        key = (m.round, m.draw_position)
        if key not in all_decided:
            all_decided[key] = m.winner_id

    # Try to propagate each decided position
    propagated = _propagate_all(session, edition, all_decided)
    stats.propagations_created = propagated

    logger.info(stats.summary())
    return stats


# =============================================================================
# Helper functions
# =============================================================================

def _resolve_player(
    session: Session,
    name: str,
    external_id: Optional[str],
    source: str,
    identity_service: PlayerIdentityService,
) -> Optional[int]:
    """
    Resolve a player name/ID to a canonical player_id.

    Follows the same pattern as backfill_historical.py:
    1. Try find_or_queue_player (exact ID, alias, fuzzy match)
    2. If no match but external_id exists, create a new player

    Args:
        session: Database session
        name: Player display name
        external_id: Tour-specific ID (ATP ID, etc.)
        source: Data source ('atp', 'wta', 'itf')
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
        )

    return player_id


def _get_player_external_id(
    session: Session,
    player_id: int,
    source: str,
) -> Optional[str]:
    from teelo.db.models import Player

    player = session.query(Player).filter(Player.id == player_id).first()
    if not player:
        return None

    if source == "atp":
        return player.atp_id
    if source == "wta":
        return player.wta_id
    if source == "itf":
        return player.itf_id
    return None


def _resolve_bye_player(
    session: Session,
    entry: ScrapedDrawEntry,
    identity_service: PlayerIdentityService,
) -> Optional[int]:
    """
    Resolve the player who received a bye.

    In a bye entry, player_a is the real player (the one who advances).
    player_b is either None or "Bye".

    Args:
        session: Database session
        entry: ScrapedDrawEntry with is_bye=True
        identity_service: Player matching service

    Returns:
        player_id of the advancing player, or None
    """
    # The real player is in player_a
    if entry.player_a_name and entry.player_a_name.lower() != "bye":
        return _resolve_player(
            session, entry.player_a_name, entry.player_a_external_id,
            entry.source, identity_service,
        )

    # Edge case: player_b has the real player (shouldn't happen with our parser)
    if entry.player_b_name and entry.player_b_name.lower() != "bye":
        return _resolve_player(
            session, entry.player_b_name, entry.player_b_external_id,
            entry.source, identity_service,
        )

    return None


def _upsert_draw_match(
    session: Session,
    entry: ScrapedDrawEntry,
    edition: TournamentEdition,
    player_a_id: int,
    player_b_id: int,
    overwrite: bool,
    seen_external_ids: set[str],
) -> Optional[Match]:
    """
    Create or update a Match row from a draw entry.

    Deduplication uses the composite (tournament_edition_id, round, draw_position)
    since draw entries don't have a single external_id.

    Args:
        session: Database session
        entry: ScrapedDrawEntry with both players known
        edition: TournamentEdition this match belongs to
        player_a_id: Resolved canonical player ID for player A
        player_b_id: Resolved canonical player ID for player B
        overwrite: If True, update existing matches

    Returns:
        Match object (new or existing), or None if skipped
    """
    # Check for existing match by draw position
    existing = session.query(Match).filter(
        Match.tournament_edition_id == edition.id,
        Match.round == entry.round,
        Match.draw_position == entry.draw_position,
    ).first()

    # Determine match status and winner
    # If match is not yet complete, status is 'upcoming' (known from draw, no schedule yet)
    # Status will change to 'scheduled' when order of play is scraped
    is_completed = bool(entry.winner_name) and bool(entry.score_raw and entry.score_raw.strip())
    status = "completed"
    winner_id = None

    if is_completed:
        # Determine winner ID from winner_name
        if entry.winner_name == entry.player_a_name:
            winner_id = player_a_id
        elif entry.winner_name == entry.player_b_name:
            winner_id = player_b_id
        else:
            # Winner name doesn't match either player — log and still mark completed
            logger.warning(
                "Winner name '%s' doesn't match either player (%s, %s) "
                "for %s #%d",
                entry.winner_name, entry.player_a_name, entry.player_b_name,
                entry.round, entry.draw_position,
            )
            # Default to player_a as winner (ATP lists winner first)
            winner_id = player_a_id
    else:
        status = "upcoming"

    # Parse score if available
    score_structured = None
    if entry.score_raw:
        try:
            parsed = parse_score(entry.score_raw)
            score_structured = parsed.to_structured()
        except ScoreParseError:
            pass

    # Generate an external_id for deduplication
    # Uses player-ID-based format when possible (compatible with results scraper)
    player_a_ext_id = entry.player_a_external_id or _get_player_external_id(
        session, player_a_id, entry.source
    )
    player_b_ext_id = entry.player_b_external_id or _get_player_external_id(
        session, player_b_id, entry.source
    )

    external_id = _make_external_id(
        year=entry.tournament_year,
        tournament_id=entry.tournament_id,
        round_code=entry.round,
        draw_position=entry.draw_position,
        player_a_ext_id=player_a_ext_id,
        player_b_ext_id=player_b_ext_id,
    )

    # Check for existing match by external_id (results may have created it already)
    if external_id:
        if external_id in seen_external_ids:
            return None
        seen_external_ids.add(external_id)

        # Check pending matches in the current session
        for pending in session.new:
            if isinstance(pending, Match) and pending.external_id == external_id:
                return None

        existing_by_external = session.query(Match).filter(
            Match.external_id == external_id
        ).first()
        if existing_by_external:
            existing = existing_by_external

    if existing and not overwrite:
        updated = False
        if existing.draw_position is None:
            existing.draw_position = entry.draw_position
            updated = True
        if existing.player_a_seed is None and entry.player_a_seed is not None:
            existing.player_a_seed = entry.player_a_seed
            updated = True
        if existing.player_b_seed is None and entry.player_b_seed is not None:
            existing.player_b_seed = entry.player_b_seed
            updated = True
        if external_id and not existing.external_id:
            # Only set if not already used by another match
            conflict = session.query(Match.id).filter(
                Match.external_id == external_id,
                Match.id != existing.id,
            ).first()
            if not conflict:
                existing.external_id = external_id
                updated = True
        return existing if updated else None

    # Estimate match date from tournament dates + round
    match_date = None
    match_date_estimated = False
    if edition.start_date and edition.end_date:
        match_date = estimate_match_date_from_round(
            round_code=entry.round,
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )
        if match_date is not None:
            match_date_estimated = True

    if existing and overwrite:
        # Update existing match
        existing.player_a_id = player_a_id
        existing.player_b_id = player_b_id
        existing.player_a_seed = entry.player_a_seed
        existing.player_b_seed = entry.player_b_seed
        existing.winner_id = winner_id
        existing.score = entry.score_raw
        existing.score_structured = score_structured
        existing.status = status
        existing.match_date = match_date
        existing.match_date_estimated = match_date_estimated
        if external_id:
            existing.external_id = external_id
        if existing.draw_position is None:
            existing.draw_position = entry.draw_position
        existing.update_temporal_order(
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )
        return existing

    # Create new match
    match = Match(
        external_id=external_id,
        source=entry.source,
        tournament_edition_id=edition.id,
        round=entry.round,
        draw_position=entry.draw_position,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        player_a_seed=entry.player_a_seed,
        player_b_seed=entry.player_b_seed,
        winner_id=winner_id,
        score=entry.score_raw,
        score_structured=score_structured,
        match_date=match_date,
        match_date_estimated=match_date_estimated,
        status=status,
    )

    # Compute temporal order
    match.update_temporal_order(
        tournament_start=edition.start_date,
        tournament_end=edition.end_date,
    )

    session.add(match)
    return match


# =============================================================================
# Result propagation
# =============================================================================

def _propagate_all(
    session: Session,
    edition: TournamentEdition,
    decided: dict[tuple[str, int], int],
) -> int:
    """
    Try to create next-round matches from all decided positions.

    For each decided position, compute the next-round slot and check if
    the other feeder is also decided. If both are decided, create the
    next-round match with both winners.

    Args:
        session: Database session
        edition: TournamentEdition context
        decided: Map of (round, position) → winner player_id
                 Includes both completed matches and byes

    Returns:
        Number of new next-round matches created
    """
    created = 0

    # Group decided positions by round
    for (round_code, position), winner_id in decided.items():
        next_round = get_next_round(round_code)
        if not next_round:
            continue  # Finals — no next round

        next_position = get_next_draw_position(position)

        # Find the other feeder position
        feeder_top, feeder_bottom = get_feeder_positions(next_position)

        # Determine which feeder we are and which is the other
        other_feeder = feeder_bottom if position == feeder_top else feeder_top
        other_winner = decided.get((round_code, other_feeder))

        if other_winner is None:
            continue  # Other feeder hasn't decided yet

        # Both feeders decided — check if next-round match already exists
        existing = session.query(Match).filter(
            Match.tournament_edition_id == edition.id,
            Match.round == next_round,
            Match.draw_position == next_position,
        ).first()

        if existing:
            continue  # Already created (from a previous run or from the draw)

        # Determine player A (top feeder) and player B (bottom feeder)
        top_winner = decided.get((round_code, feeder_top))
        bottom_winner = decided.get((round_code, feeder_bottom))

        if not top_winner or not bottom_winner:
            continue  # Shouldn't happen, but be safe

        # Generate external_id
        external_id = (
            f"draw_{edition.year}_{edition.tournament.tournament_code}"
            f"_{next_round}_{next_position}"
        )

        # Estimate date
        match_date = None
        match_date_estimated = False
        if edition.start_date and edition.end_date:
            match_date = estimate_match_date_from_round(
                round_code=next_round,
                tournament_start=edition.start_date,
                tournament_end=edition.end_date,
            )
            if match_date:
                match_date_estimated = True

        match = Match(
            external_id=external_id,
            source="atp",  # Draw-propagated matches inherit the source
            tournament_edition_id=edition.id,
            round=next_round,
            draw_position=next_position,
            player_a_id=top_winner,
            player_b_id=bottom_winner,
            status="upcoming",  # Known from draw, no schedule yet
            match_date=match_date,
            match_date_estimated=match_date_estimated,
        )
        match.update_temporal_order(
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )

        session.add(match)
        created += 1
        logger.info(
            "Propagated: %s #%d created (players %d vs %d)",
            next_round, next_position, top_winner, bottom_winner,
        )

    return created


def propagate_draw_result(
    session: Session,
    completed_match: Match,
) -> Optional[Match]:
    """
    Propagate a single completed match result to the next round.

    Call this when a match with a draw_position completes (e.g., from
    live score updates). Checks if the other feeder is also complete,
    and if so, creates the next-round match.

    This is the "event-driven" counterpart to _propagate_all() which
    does a batch check during ingestion.

    Args:
        session: Database session
        completed_match: The match that just completed (must have
                        draw_position, winner_id, and tournament_edition_id)

    Returns:
        Newly created next-round Match, or None if not yet possible
        (other feeder hasn't completed)
    """
    if not completed_match.draw_position:
        return None
    if not completed_match.winner_id:
        return None
    if not completed_match.tournament_edition_id:
        return None

    round_code = completed_match.round
    position = completed_match.draw_position
    edition_id = completed_match.tournament_edition_id

    next_round = get_next_round(round_code)
    if not next_round:
        return None  # This was the Final

    next_position = get_next_draw_position(position)

    # Check if next-round match already exists
    existing = session.query(Match).filter(
        Match.tournament_edition_id == edition_id,
        Match.round == next_round,
        Match.draw_position == next_position,
    ).first()

    if existing:
        return None  # Already exists

    # Find the other feeder match
    feeder_top, feeder_bottom = get_feeder_positions(next_position)
    other_position = feeder_bottom if position == feeder_top else feeder_top

    other_match = session.query(Match).filter(
        Match.tournament_edition_id == edition_id,
        Match.round == round_code,
        Match.draw_position == other_position,
        Match.winner_id.isnot(None),
    ).first()

    if not other_match:
        # Other feeder hasn't completed — check for a bye at that position
        # Byes don't create Match rows, so we can't find them this way.
        # In the current design, byes are handled during ingestion via
        # _propagate_all(). If this is called outside of ingestion, byes
        # won't be detected here. This is fine — byes only exist during
        # initial draw setup, not during live updates.
        return None

    # Both feeders complete — create next-round match
    # Top feeder's winner is player_a, bottom feeder's winner is player_b
    if position == feeder_top:
        player_a_id = completed_match.winner_id
        player_b_id = other_match.winner_id
    else:
        player_a_id = other_match.winner_id
        player_b_id = completed_match.winner_id

    edition = session.query(TournamentEdition).get(edition_id)

    external_id = (
        f"draw_{edition.year}_{edition.tournament.tournament_code}"
        f"_{next_round}_{next_position}"
    )

    # Estimate date
    match_date = None
    match_date_estimated = False
    if edition and edition.start_date and edition.end_date:
        match_date = estimate_match_date_from_round(
            round_code=next_round,
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )
        if match_date:
            match_date_estimated = True

    new_match = Match(
        external_id=external_id,
        source=completed_match.source,
        tournament_edition_id=edition_id,
        round=next_round,
        draw_position=next_position,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        status="upcoming",  # Known from draw, no schedule yet
        match_date=match_date,
        match_date_estimated=match_date_estimated,
    )
    new_match.update_temporal_order(
        tournament_start=edition.start_date if edition else None,
        tournament_end=edition.end_date if edition else None,
    )

    session.add(new_match)
    session.flush()

    logger.info(
        "Propagated result: %s #%d → %s #%d (players %d vs %d)",
        round_code, position, next_round, next_position,
        player_a_id, player_b_id,
    )

    return new_match
