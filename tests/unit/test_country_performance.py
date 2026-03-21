from datetime import date

from teelo.features.groups.country_performance import CountryPerformanceFeatures
from teelo.features.state import MatchContext, PlayerState


def _state_with_country_record(
    wins_total: int = 20,
    losses_total: int = 10,
    country_record: dict | None = None,
    region_record: dict | None = None,
) -> PlayerState:
    state = PlayerState(player_id=1)
    state.wins_total = wins_total
    state.losses_total = losses_total
    if country_record:
        state.country_record = country_record
    if region_record:
        state.region_record = region_record
    return state


def _ctx(
    country_ioc: str | None = "GBR",
    player_a_nationality: str | None = "ESP",
    player_b_nationality: str | None = "GBR",
) -> MatchContext:
    return MatchContext(
        match_id=1, match_date=date(2026, 3, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
        tournament_country_ioc=country_ioc,
        player_a_nationality=player_a_nationality,
        player_b_nationality=player_b_nationality,
    )


def test_country_win_rate() -> None:
    group = CountryPerformanceFeatures()
    state_a = _state_with_country_record(
        country_record={"GBR": (4, 1)},
        region_record={"Europe": (10, 5)},
    )
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    assert features["country_win_rate_a"] == 4 / 5
    assert features["country_matches_a"] == 5


def test_country_win_rate_small_sample() -> None:
    group = CountryPerformanceFeatures()
    state_a = _state_with_country_record(
        country_record={"GBR": (2, 1)},
    )
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    assert features["country_win_rate_a"] == 2 / 3
    assert features["country_matches_a"] == 3


def test_country_delta() -> None:
    group = CountryPerformanceFeatures()
    state_a = _state_with_country_record(
        wins_total=20, losses_total=10,
        country_record={"GBR": (4, 1)},
    )
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    expected_delta = (4 / 5) - (20 / 30)
    assert features["country_delta_a"] is not None
    assert abs(features["country_delta_a"] - expected_delta) < 0.001


def test_is_home() -> None:
    group = CountryPerformanceFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx(
        player_a_nationality="ESP", player_b_nationality="GBR",
    ))
    assert features["is_home_a"] == 0.0
    assert features["is_home_b"] == 1.0


def test_no_country_on_context() -> None:
    group = CountryPerformanceFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx(country_ioc=None))
    assert features["country_win_rate_a"] == 0.5
    assert features["country_matches_a"] == 0
    assert features["is_home_a"] == 0.0


def test_feature_names_count() -> None:
    group = CountryPerformanceFeatures()
    assert len(group.feature_names()) == 14
    assert group.name == "country_performance"
