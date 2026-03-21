from datetime import date
from teelo.features.groups.elo import EloCoreFeatures
from teelo.features.state import MatchContext, PlayerState


def _ctx(surface: str | None = "Clay") -> MatchContext:
    return MatchContext(
        match_id=1, match_date=date(2026, 3, 1), surface=surface,
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
    )


def test_surface_gap() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1, elo_current=1600.0)
    state_a.surface_elo["Clay"] = 1650.0
    state_b = PlayerState(player_id=2, elo_current=1500.0)
    state_b.surface_elo["Clay"] = 1500.0
    features = group.compute(state_a, state_b, _ctx("Clay"))
    assert features["surface_gap_a"] == 50.0
    assert features["surface_gap_b"] == 0.0


def test_off_surface_elo() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1, elo_current=1600.0)
    state_a.surface_elo["Clay"] = 1650.0
    state_a.surface_elo["Hard"] = 1580.0
    state_a.surface_elo["Grass"] = 1560.0
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx("Clay"))
    assert features["off_surface_elo_a"] == 1570.0


def test_surface_gap_none_when_no_surface() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx(surface=None))
    assert features["surface_gap_a"] is None
    assert features["off_surface_elo_a"] is None


def test_off_surface_elo_none_when_single_surface() -> None:
    group = EloCoreFeatures()
    state_a = PlayerState(player_id=1, elo_current=1600.0)
    state_a.surface_elo["Clay"] = 1650.0
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx("Clay"))
    assert features["surface_gap_a"] == 50.0
    assert features["off_surface_elo_a"] is None
