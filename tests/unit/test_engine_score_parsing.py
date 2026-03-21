from datetime import date
from teelo.features.engine import _compute_clutch_score, _specialist_score
from teelo.features.state import MatchRecord, PlayerState


def test_specialist_score_no_surface() -> None:
    state = PlayerState(player_id=1)
    assert _specialist_score(state, None) is None


def test_specialist_score_with_surface() -> None:
    state = PlayerState(player_id=1, elo_current=1600.0)
    state.surface_elo["Clay"] = 1650.0
    assert _specialist_score(state, "Clay") == 50.0


def test_clutch_score_insufficient_matches() -> None:
    state = PlayerState(player_id=1)
    assert _compute_clutch_score(state) is None


def test_clutch_score_neutral_when_no_sub_events() -> None:
    state = PlayerState(player_id=1)
    for i in range(10):
        state.matches.append(MatchRecord(
            temporal_order=i, won=True, surface="Hard", level_code="A",
            games_won=6, games_lost=3, tournament_edition_id=1,
            tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        ))
    score = _compute_clutch_score(state)
    assert score is not None
    assert abs(score - 0.5) < 0.01
