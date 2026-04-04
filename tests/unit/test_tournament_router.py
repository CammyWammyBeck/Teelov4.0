import asyncio
import json
from datetime import date, timedelta

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from teelo.db.models import Match, Player, Tournament, TournamentEdition, TournamentForecastNode, TournamentForecastRun
from teelo.web.routers.tournaments import (
    _build_edition_history_payload,
    _filter_editions_with_finals,
    api_tournaments,
)


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(_element, _compiler, **_kwargs):
    return "JSON"


def test_build_edition_history_payload_skips_editions_without_finals():
    tournament = Tournament(
        tournament_code="indian-wells",
        name="Indian Wells",
        tour="WTA",
        gender="women",
        level="WTA 1000",
        surface="Hard",
    )
    edition_with_final = TournamentEdition(id=1, year=2026, surface="Hard")
    edition_without_final = TournamentEdition(id=2, year=2025, surface="Hard")

    player_a = Player(id=10, canonical_name="Player A", wta_id="100")
    player_b = Player(id=11, canonical_name="Player B", wta_id="101")
    final = Match(
        tournament_edition_id=1,
        round="F",
        player_a_id=player_a.id,
        player_b_id=player_b.id,
        player_a=player_a,
        player_b=player_b,
        winner_id=player_a.id,
        winner=player_a,
        score="6-4 6-4",
        status="completed",
        source="wta",
    )

    payload = _build_edition_history_payload(
        editions=[edition_with_final, edition_without_final],
        finals_by_edition={1: final},
        tournament=tournament,
        tour_key="WTA",
        tournament_code="indian-wells",
    )

    assert payload == [
        {
            "year": 2026,
            "champion": "Player A",
            "champion_id": 10,
            "runner_up": "Player B",
            "runner_up_id": 11,
            "score": "6-4 6-4",
            "surface": "Hard",
            "url": "/tournaments/wta/indian-wells/2026",
        }
    ]


def test_filter_editions_with_finals_returns_only_real_editions():
    edition_with_final = TournamentEdition(id=1, year=2026)
    edition_without_final = TournamentEdition(id=2, year=2025)
    final = Match(
        tournament_edition_id=1,
        round="F",
        source="wta",
        player_a_id=1,
        player_b_id=2,
        status="completed",
    )

    filtered = _filter_editions_with_finals(
        editions=[edition_with_final, edition_without_final],
        finals_by_edition={1: final},
    )

    assert [edition.year for edition in filtered] == [2026]


def test_api_tournaments_supports_status_and_year_filters():
    engine = create_engine("sqlite:///:memory:")
    Tournament.__table__.create(engine)
    TournamentEdition.__table__.create(engine)
    Player.__table__.create(engine)
    Match.__table__.create(engine)
    TournamentForecastRun.__table__.create(engine)
    TournamentForecastNode.__table__.create(engine)
    session_factory = sessionmaker(bind=engine)
    db_session = session_factory()

    today = date.today()
    next_year = today.year + 1

    current_tournament = Tournament(
        id=1,
        tournament_code="miami-open",
        name="Miami Open",
        tour="ATP",
        gender="men",
        level="Masters 1000",
        surface="Hard",
        city="Miami",
        country="USA",
    )
    current_edition = TournamentEdition(
        id=11,
        tournament_id=1,
        tournament=current_tournament,
        year=today.year,
        start_date=today - timedelta(days=1),
        end_date=today + timedelta(days=1),
    )

    upcoming_tournament = Tournament(
        id=2,
        tournament_code="rome-open",
        name="Rome Open",
        tour="WTA",
        gender="women",
        level="WTA 1000",
        surface="Clay",
        city="Rome",
        country="Italy",
    )
    upcoming_edition = TournamentEdition(
        id=12,
        tournament_id=2,
        tournament=upcoming_tournament,
        year=next_year,
        start_date=date(next_year, 5, 10),
        end_date=date(next_year, 5, 17),
    )

    window_upcoming_tournament = Tournament(
        id=4,
        tournament_code="sunshine-open",
        name="Sunshine Open",
        tour="ATP",
        gender="men",
        level="ATP 250",
        surface="Hard",
        city="Adelaide",
        country="Australia",
    )
    window_upcoming_edition = TournamentEdition(
        id=14,
        tournament_id=4,
        tournament=window_upcoming_tournament,
        year=today.year,
        start_date=today - timedelta(days=1),
        end_date=today + timedelta(days=1),
    )

    completed_tournament = Tournament(
        id=3,
        tournament_code="aus-open",
        name="Australian Open",
        tour="ATP",
        gender="men",
        level="Grand Slam",
        surface="Hard",
        city="Melbourne",
        country="Australia",
    )
    completed_edition = TournamentEdition(
        id=13,
        tournament_id=3,
        tournament=completed_tournament,
        year=today.year - 1,
        start_date=date(today.year - 1, 1, 12),
        end_date=date(today.year - 1, 1, 26),
    )

    winner = Player(id=101, canonical_name="Champion Player", gender="men")
    runner_up = Player(id=102, canonical_name="Runner Up", gender="men")

    try:
        db_session.add_all(
            [
                current_tournament,
                current_edition,
                upcoming_tournament,
                upcoming_edition,
                window_upcoming_tournament,
                window_upcoming_edition,
                completed_tournament,
                completed_edition,
                winner,
                runner_up,
                Match(
                    tournament_edition_id=current_edition.id,
                    round="QF",
                    status="completed",
                    source="atp",
                    player_a_id=winner.id,
                    player_b_id=runner_up.id,
                ),
                Match(
                    tournament_edition_id=completed_edition.id,
                    round="F",
                    status="completed",
                    source="atp",
                    player_a_id=winner.id,
                    player_b_id=runner_up.id,
                    winner_id=winner.id,
                    score="6-4 6-3",
                ),
            ]
        )
        db_session.commit()

        response = asyncio.run(
            api_tournaments(
                db=db_session,
                tour=None,
                gender=None,
                surface=None,
                level=None,
                year=None,
                status=None,
                tournament=None,
                page=1,
                per_page=24,
            )
        )
        payload = json.loads(response.body)
        assert payload["total"] == 4
        assert [item["status"] for item in payload["tournaments"]] == [
            "current",
            "upcoming",
            "upcoming",
            "completed",
        ]
        assert payload["tournaments"][1]["tournament_name"] == "Sunshine Open"

        completed_response = asyncio.run(
            api_tournaments(
                db=db_session,
                tour=None,
                gender=None,
                surface=None,
                level=None,
                year=None,
                status="completed",
                tournament=None,
                page=1,
                per_page=24,
            )
        )
        completed_payload = json.loads(completed_response.body)
        assert completed_payload["total"] == 1
        assert completed_payload["tournaments"][0]["tournament_name"] == "Australian Open"
        assert completed_payload["tournaments"][0]["winner_name"] == "Champion Player"

        year_response = asyncio.run(
            api_tournaments(
                db=db_session,
                tour=None,
                gender=None,
                surface=None,
                level=None,
                year=str(next_year),
                status=None,
                tournament=None,
                page=1,
                per_page=24,
            )
        )
        year_payload = json.loads(year_response.body)
        assert year_payload["total"] == 1
        assert year_payload["tournaments"][0]["tournament_name"] == "Rome Open"
    finally:
        db_session.close()
