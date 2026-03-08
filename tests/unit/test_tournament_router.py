from teelo.db.models import Match, Player, Tournament, TournamentEdition
from teelo.web.routers.tournaments import _build_edition_history_payload, _filter_editions_with_finals


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
    final = Match(tournament_edition_id=1, round="F", source="wta", player_a_id=1, player_b_id=2, status="completed")

    filtered = _filter_editions_with_finals(
        editions=[edition_with_final, edition_without_final],
        finals_by_edition={1: final},
    )

    assert [edition.year for edition in filtered] == [2026]
