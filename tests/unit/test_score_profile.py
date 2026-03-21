from datetime import date

from teelo.features.groups.score_profile import ScoreProfileFeatures
from teelo.features.state import MatchContext, MatchRecord, PlayerState


def _record(
    *,
    temporal_order: int,
    won: bool,
    games_won: int = 6,
    games_lost: int = 3,
    sets_won: int = 2,
    sets_lost: int = 0,
    tiebreaks_played: int = 0,
    tiebreaks_won: int = 0,
    deciding_set_played: bool = False,
    straight_sets: bool = False,
    close_match: bool = False,
    first_set_lost: bool = False,
    opponent_clutch_score: float | None = None,
) -> MatchRecord:
    return MatchRecord(
        temporal_order=temporal_order, won=won, surface="Hard",
        level_code="A", games_won=games_won, games_lost=games_lost,
        tournament_edition_id=1, tournament_id=1,
        match_date=date(2026, 1, 1), opponent_id=2,
        sets_won=sets_won, sets_lost=sets_lost,
        tiebreaks_played=tiebreaks_played, tiebreaks_won=tiebreaks_won,
        deciding_set_played=deciding_set_played,
        straight_sets=straight_sets, close_match=close_match,
        first_set_lost=first_set_lost,
        opponent_clutch_score=opponent_clutch_score,
    )


def _ctx() -> MatchContext:
    return MatchContext(
        match_id=1, match_date=date(2026, 3, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        match_date_estimated=False,
    )


def test_returns_default_with_no_matches() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    features = group.compute(state_a, state_b, _ctx())
    assert features["straight_sets_rate_8_a"] == 0.5
    assert features["tiebreak_win_rate_8_a"] == 0.5
    assert features["game_diff_avg_8_a"] == 0.0
    assert features["tiebreaks_played_8_a"] == 0


def test_window_8_straight_sets_rate() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(4):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            straight_sets=(i < 2),
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["straight_sets_rate_8_a"] == 0.5


def test_tiebreak_win_rate_default_when_no_tiebreaks() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(5):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            tiebreaks_played=0, tiebreaks_won=0,
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["tiebreak_win_rate_8_a"] == 0.5
    assert features["tiebreaks_played_8_a"] == 0


def test_window_64_deciding_set_win_rate() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=(i < 7),
            deciding_set_played=(i < 6),
            sets_won=2, sets_lost=1,
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["deciding_sets_played_64_a"] == 6
    assert features["deciding_set_win_rate_64_a"] == 1.0


def test_comeback_rate() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=(i < 5),
            first_set_lost=(i < 8),
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["first_sets_lost_64_a"] == 8
    assert features["comeback_rate_64_a"] == 5 / 8


def test_clutch_matchup_features() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            opponent_clutch_score=0.60 if i < 5 else 0.30,
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["vs_clutch_matches_a"] == 5
    assert features["vs_clutch_win_rate_a"] == 1.0
    assert features["vs_non_clutch_matches_a"] == 5
    assert features["vs_non_clutch_win_rate_a"] == 1.0


def test_clutch_matchup_all_none_opponents() -> None:
    group = ScoreProfileFeatures()
    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)
    for i in range(10):
        state_a.matches.append(_record(
            temporal_order=i, won=True,
            opponent_clutch_score=None,
        ))
    features = group.compute(state_a, state_b, _ctx())
    assert features["vs_clutch_win_rate_a"] == 0.5
    assert features["vs_clutch_matches_a"] == 0
    assert features["vs_normal_clutch_matches_a"] == 0
    assert features["vs_non_clutch_matches_a"] == 0


def test_feature_names_count() -> None:
    group = ScoreProfileFeatures()
    names = group.feature_names()
    assert len(names) == 56
    assert group.name == "score_profile"
