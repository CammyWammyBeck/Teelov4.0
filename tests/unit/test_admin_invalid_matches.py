from teelo.db.models import Match
from teelo.web.services.main_handlers import _invalid_match_reasons


def _match(**overrides):
    data = {
        "source": "atp",
        "player_a_id": 1,
        "player_b_id": 2,
        "status": "completed",
        "winner_id": 1,
    }
    data.update(overrides)
    return Match(**data)


def test_invalid_match_reasons_detects_core_cases():
    match = _match(player_b_id=1, winner_id=3)
    reasons = _invalid_match_reasons(match)
    assert "player_a is the same as player_b" in reasons
    assert "winner is neither player_a nor player_b" in reasons


def test_invalid_match_reasons_detects_status_winner_mismatches():
    no_winner_terminal = _match(winner_id=None, status="retired")
    assert "terminal status with no winner" in _invalid_match_reasons(no_winner_terminal)

    winner_non_terminal = _match(status="scheduled", winner_id=1)
    assert "winner set on a non-terminal status" in _invalid_match_reasons(winner_non_terminal)
