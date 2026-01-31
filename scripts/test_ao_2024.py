#!/usr/bin/env python3
"""
Test script: Scrape 2024 Australian Open and store in database.

This script tests the full data pipeline:
1. ATP scraper fetches match data
2. Player identity system creates/matches players
3. Matches are stored in the unified matches table

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_ao_2024.py

Expected output:
    - Tournament created in database
    - Players created/matched
    - Matches stored with scores and player links
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import func

from teelo.db import get_session, Player, Match, Tournament, TournamentEdition
from teelo.scrape.atp import ATPScraper
from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.players.identity import PlayerIdentityService


async def create_tournament_if_needed(session) -> TournamentEdition:
    """
    Create Australian Open tournament and 2024 edition if they don't exist.

    Returns:
        TournamentEdition for 2024 Australian Open
    """
    # Check if tournament exists
    tournament = session.query(Tournament).filter(
        Tournament.tournament_code == "australian-open",
        Tournament.tour == "ATP"
    ).first()

    if not tournament:
        print("Creating Australian Open tournament...")
        tournament = Tournament(
            tournament_code="australian-open",
            name="Australian Open",
            tour="ATP",
            level="Grand Slam",
            city="Melbourne",
            country="Australia",
            country_ioc="AUS",
            surface="Hard",
            indoor_outdoor="Outdoor",
        )
        session.add(tournament)
        session.flush()  # Get the ID
        print(f"  Created tournament: {tournament}")
    else:
        print(f"Tournament already exists: {tournament}")

    # Check if 2024 edition exists
    edition = session.query(TournamentEdition).filter(
        TournamentEdition.tournament_id == tournament.id,
        TournamentEdition.year == 2024
    ).first()

    if not edition:
        print("Creating 2024 edition...")
        edition = TournamentEdition(
            tournament_id=tournament.id,
            year=2024,
            start_date=datetime(2024, 1, 14),
            end_date=datetime(2024, 1, 28),
            surface="Hard",
            draw_size=128,
        )
        session.add(edition)
        session.flush()
        print(f"  Created edition: {edition}")
    else:
        print(f"Edition already exists: {edition}")

    return edition


async def process_scraped_match(
    session,
    scraped_match,
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
) -> Match | None:
    """
    Process a scraped match and store it in the database.

    Args:
        session: Database session
        scraped_match: ScrapedMatch from the scraper
        edition: TournamentEdition to link the match to
        identity_service: PlayerIdentityService for player matching

    Returns:
        Match object if created, None if skipped
    """
    # Find or create player A
    player_a_id, status_a = identity_service.find_or_queue_player(
        name=scraped_match.player_a_name,
        source="atp",
        external_id=scraped_match.player_a_external_id,
    )

    # Auto-create if queued and we have an ATP ID (initial data load)
    if not player_a_id and scraped_match.player_a_external_id:
        player_a_id = identity_service.create_player(
            name=scraped_match.player_a_name,
            source="atp",
            external_id=scraped_match.player_a_external_id,
            nationality=scraped_match.player_a_nationality,
        )
        status_a = "created"

    if not player_a_id:
        print(f"  ! Player A queued for review: {scraped_match.player_a_name}")
        return None

    # Find or create player B
    player_b_id, status_b = identity_service.find_or_queue_player(
        name=scraped_match.player_b_name,
        source="atp",
        external_id=scraped_match.player_b_external_id,
    )

    # Auto-create if queued and we have an ATP ID (initial data load)
    if not player_b_id and scraped_match.player_b_external_id:
        player_b_id = identity_service.create_player(
            name=scraped_match.player_b_name,
            source="atp",
            external_id=scraped_match.player_b_external_id,
            nationality=scraped_match.player_b_nationality,
        )
        status_b = "created"

    if not player_b_id:
        print(f"  ! Player B queued for review: {scraped_match.player_b_name}")
        return None

    # Check if match already exists
    existing = session.query(Match).filter(
        Match.external_id == scraped_match.external_id
    ).first()

    if existing:
        print(f"  - Match already exists: {scraped_match.external_id}")
        return existing

    # Parse score for structured format
    score_structured = None
    try:
        parsed = parse_score(scraped_match.score_raw)
        score_structured = parsed.to_structured()
    except ScoreParseError as e:
        print(f"  ! Could not parse score '{scraped_match.score_raw}': {e}")

    # Determine winner
    # In ATP results, player A is typically the winner
    winner_id = player_a_id

    # Parse match date
    match_date = None
    if scraped_match.match_date:
        try:
            match_date = datetime.strptime(scraped_match.match_date, "%Y-%m-%d").date()
        except ValueError:
            pass

    # Create match
    match = Match(
        external_id=scraped_match.external_id,
        source=scraped_match.source,
        tournament_edition_id=edition.id,
        round=scraped_match.round,
        match_number=scraped_match.match_number,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        winner_id=winner_id,
        score=scraped_match.score_raw,
        score_structured=score_structured,
        match_date=match_date,
        status=scraped_match.status,
        retirement_set=scraped_match.retirement_set,
    )

    # Compute temporal order for chronological sorting
    # Fallback chain: match_date -> scheduled_date -> sibling_date -> tournament dates
    match.update_temporal_order(
        tournament_start=edition.start_date,
        tournament_end=edition.end_date,
    )

    session.add(match)
    return match


async def main():
    """
    Main test function.

    Scrapes 2024 Australian Open and stores data in database.
    """
    print("=" * 60)
    print("Teelo v4.0 - Australian Open 2024 Scraper Test")
    print("=" * 60)
    print()

    # Track statistics
    stats = {
        "matches_scraped": 0,
        "matches_created": 0,
        "matches_skipped": 0,
        "players_created": 0,
        "players_queued": 0,
        "errors": 0,
    }

    with get_session() as session:
        # Create tournament and edition
        print("[1/4] Setting up tournament...")
        edition = await create_tournament_if_needed(session)
        session.commit()
        print()

        # Initialize player identity service
        print("[2/4] Initializing player identity service...")
        identity_service = PlayerIdentityService(session)

        # Count players before
        players_before = session.query(func.count(Player.id)).scalar()
        print(f"  Players in database: {players_before}")
        print()

        # Scrape the tournament
        print("[3/4] Scraping 2024 Australian Open...")
        print("  (This may take a minute - scraping with Playwright)")
        print()

        try:
            # Use non-headless mode to bypass Cloudflare detection
            async with ATPScraper(headless=False) as scraper:
                async for scraped_match in scraper.scrape_tournament_results(
                    "australian-open",
                    2024,
                    include_qualifying=False  # Start with main draw only
                ):
                    stats["matches_scraped"] += 1

                    try:
                        match = await process_scraped_match(
                            session,
                            scraped_match,
                            edition,
                            identity_service,
                        )

                        if match:
                            stats["matches_created"] += 1
                            print(f"  + {scraped_match.round}: {scraped_match.player_a_name} d. {scraped_match.player_b_name} {scraped_match.score_raw}")
                        else:
                            stats["matches_skipped"] += 1

                    except Exception as e:
                        stats["errors"] += 1
                        print(f"  ! Error processing match: {e}")
                        continue

                    # Commit periodically
                    if stats["matches_created"] % 10 == 0:
                        session.commit()

            # Final commit
            session.commit()

        except Exception as e:
            print(f"\nError during scraping: {e}")
            stats["errors"] += 1

        print()

        # Count players after
        players_after = session.query(func.count(Player.id)).scalar()
        stats["players_created"] = players_after - players_before

        # Check review queue
        from teelo.db.models import PlayerReviewQueue
        stats["players_queued"] = session.query(func.count(PlayerReviewQueue.id)).filter(
            PlayerReviewQueue.status == "pending"
        ).scalar()

        # Summary
        print("[4/4] Summary")
        print("=" * 40)
        print(f"  Matches scraped:     {stats['matches_scraped']}")
        print(f"  Matches created:     {stats['matches_created']}")
        print(f"  Matches skipped:     {stats['matches_skipped']}")
        print(f"  Players created:     {stats['players_created']}")
        print(f"  Players in queue:    {stats['players_queued']}")
        print(f"  Errors:              {stats['errors']}")
        print()

        # Verify data
        print("Verification:")
        total_matches = session.query(func.count(Match.id)).filter(
            Match.tournament_edition_id == edition.id
        ).scalar()
        print(f"  Total matches in DB for AO 2024: {total_matches}")

        # Show a sample match
        sample = session.query(Match).filter(
            Match.tournament_edition_id == edition.id,
            Match.round == "F"
        ).first()

        if sample:
            print(f"\n  Final match:")
            print(f"    {sample.player_a.canonical_name} vs {sample.player_b.canonical_name}")
            print(f"    Score: {sample.score}")
            print(f"    Winner: {sample.winner.canonical_name if sample.winner else 'Unknown'}")

        print()
        print("Test complete!")


if __name__ == "__main__":
    asyncio.run(main())
