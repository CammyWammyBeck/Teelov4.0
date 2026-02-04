#!/usr/bin/env python3
"""
Check whether schedule fields are being saved for a tournament edition.

Usage:
    python scripts/check_schedule_saved.py --tournament abu-dhabi --year 2026
"""

import argparse
from datetime import date

from teelo.db.session import get_session
from teelo.db.models import Match, Tournament, TournamentEdition


def main() -> None:
    parser = argparse.ArgumentParser(description="Check schedule fields for a tournament edition")
    parser.add_argument("--tournament", required=True, help="Tournament code/slug (e.g., abu-dhabi)")
    parser.add_argument("--year", type=int, default=date.today().year, help="Year (e.g., 2026)")
    args = parser.parse_args()

    with get_session() as session:
        tournament = session.query(Tournament).filter(
            Tournament.tournament_code == args.tournament
        ).first()
        if not tournament:
            print(f"No tournament found for code: {args.tournament}")
            return

        edition = session.query(TournamentEdition).filter(
            TournamentEdition.tournament_id == tournament.id,
            TournamentEdition.year == args.year,
        ).first()
        if not edition:
            print(f"No edition found for {args.tournament} {args.year}")
            return

        total = session.query(Match).filter(
            Match.tournament_edition_id == edition.id
        ).count()
        scheduled_date_count = session.query(Match).filter(
            Match.tournament_edition_id == edition.id,
            Match.scheduled_date.isnot(None),
        ).count()
        scheduled_datetime_count = session.query(Match).filter(
            Match.tournament_edition_id == edition.id,
            Match.scheduled_datetime.isnot(None),
        ).count()
        scheduled_status = session.query(Match).filter(
            Match.tournament_edition_id == edition.id,
            Match.status == "scheduled",
        ).count()

        print(f"Tournament: {tournament.name} ({tournament.tournament_code})")
        print(f"Edition: {edition.year} (id={edition.id})")
        print(f"Total matches: {total}")
        print(f"Matches with scheduled_date: {scheduled_date_count}")
        print(f"Matches with scheduled_datetime: {scheduled_datetime_count}")
        print(f"Matches with status='scheduled': {scheduled_status}")

        sample = session.query(Match).filter(
            Match.tournament_edition_id == edition.id,
            Match.scheduled_date.isnot(None),
        ).order_by(Match.scheduled_date, Match.scheduled_datetime).limit(10).all()
        if not sample:
            print("No scheduled matches found.")
            return

        print("\nSample scheduled matches:")
        for m in sample:
            print(
                f"  id={m.id} round={m.round} "
                f"date={m.scheduled_date} time={m.scheduled_datetime} "
                f"court={m.court} status={m.status} ext={m.external_id}"
            )


if __name__ == "__main__":
    main()
