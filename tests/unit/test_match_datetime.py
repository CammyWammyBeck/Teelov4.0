from datetime import date, datetime
from unittest.mock import MagicMock

from teelo.utils.geo import city_country_to_timezone
from teelo.web.services.match_service import serialize_match


def _make_mock_match(
    *,
    match_id=1,
    match_date=None,
    scheduled_date=None,
    scheduled_datetime=None,
    tournament_timezone=None,
    status="scheduled",
):
    """Create a minimal mock Match object for serialize_match testing."""
    match = MagicMock()
    match.id = match_id
    match.match_date = match_date
    match.scheduled_date = scheduled_date
    match.match_datetime = None
    match.scheduled_datetime = scheduled_datetime
    match.status = status
    match.score = None
    match.winner_id = None
    match.round = "R32"
    match.player_a_seed = None
    match.player_b_seed = None
    match.prediction_a = None
    match.temporal_order = 1

    match.elo_pre_player_a = None
    match.elo_pre_player_b = None
    match.elo_post_player_a = None
    match.elo_post_player_b = None

    pa = MagicMock()
    pa.id = 100
    pa.canonical_name = "Player A"
    pb = MagicMock()
    pb.id = 200
    pb.canonical_name = "Player B"
    match.player_a = pa
    match.player_a_id = 100
    match.player_b = pb
    match.player_b_id = 200

    tournament = MagicMock()
    tournament.tour = "ATP"
    tournament.gender = "men"
    tournament.name = "Test Open"
    tournament.tournament_code = "test"
    tournament.level = "ATP 250"
    tournament.surface = "Hard"
    tournament.timezone = tournament_timezone

    edition = MagicMock()
    edition.tournament = tournament
    edition.year = 2026
    edition.surface = None

    match.tournament_edition = edition
    return match


def test_serialize_match_with_timezone_produces_utc():
    """When scheduled_datetime and timezone exist, output match_datetime_utc."""
    match = _make_mock_match(
        scheduled_date=date(2026, 3, 19),
        scheduled_datetime=datetime(2026, 3, 19, 10, 0),
        tournament_timezone="America/New_York",
    )
    result = serialize_match(match)
    assert result["match_datetime_utc"] == "2026-03-19T14:00:00Z"
    assert result["has_exact_time"] is True
    assert result["match_date_display"] == "19 Mar 2026"


def test_serialize_match_without_timezone_no_utc():
    """When no timezone, has_exact_time is False but date-based UTC is synthesized."""
    match = _make_mock_match(
        scheduled_date=date(2026, 3, 19),
        scheduled_datetime=datetime(2026, 3, 19, 10, 0),
        tournament_timezone=None,
    )
    result = serialize_match(match)
    # Without timezone we can't convert exact time, but date is still available
    assert result["match_datetime_utc"] == "2026-03-19T23:59:00Z"
    assert result["has_exact_time"] is False


def test_serialize_match_completed_no_time():
    """Completed matches show date via match_datetime_utc with has_exact_time=False."""
    match = _make_mock_match(
        match_date=date(2026, 3, 18),
        status="completed",
    )
    result = serialize_match(match)
    assert result["match_date_display"] == "18 Mar 2026"
    assert result["has_exact_time"] is False


def test_city_country_to_timezone_known_city():
    assert city_country_to_timezone("Miami", "United States") == "America/New_York"


def test_city_country_to_timezone_european():
    assert city_country_to_timezone("Paris", "France") == "Europe/Paris"


def test_city_country_to_timezone_australian():
    assert city_country_to_timezone("Melbourne", "Australia") == "Australia/Melbourne"


def test_city_country_to_timezone_none_inputs():
    assert city_country_to_timezone(None, None) is None
    assert city_country_to_timezone("", "") is None


def test_city_country_to_timezone_unknown_city():
    assert city_country_to_timezone("Nowhereville", "Fantasyland") is None
