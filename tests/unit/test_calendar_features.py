import math
from datetime import date
from teelo.features.groups.context import ContextFeatures
from teelo.features.state import MatchContext, PlayerState


def _ctx(match_date: date | None = date(2026, 6, 15)) -> MatchContext:
    return MatchContext(
        match_id=1, match_date=match_date, surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
    )


def test_month_sin_cos_june() -> None:
    group = ContextFeatures()
    state = PlayerState(player_id=1)
    features = group.compute(state, state, _ctx(date(2026, 6, 15)))
    expected_sin = math.sin(2 * math.pi * 6 / 12)
    expected_cos = math.cos(2 * math.pi * 6 / 12)
    assert abs(features["month_sin"] - expected_sin) < 0.001
    assert abs(features["month_cos"] - expected_cos) < 0.001


def test_year_progress_midyear() -> None:
    group = ContextFeatures()
    state = PlayerState(player_id=1)
    d = date(2026, 7, 2)
    features = group.compute(state, state, _ctx(d))
    assert abs(features["year_progress"] - 183 / 365) < 0.01


def test_calendar_none_when_no_date() -> None:
    group = ContextFeatures()
    state = PlayerState(player_id=1)
    features = group.compute(state, state, _ctx(match_date=None))
    assert features["month_sin"] is None
    assert features["month_cos"] is None
    assert features["year_progress"] is None
