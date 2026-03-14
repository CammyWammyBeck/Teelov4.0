from datetime import date

from teelo.features.state import MatchContext, MatchRecord, PlayerState


def _record(
    *,
    temporal_order: int,
    won: bool,
    match_date: date,
    opponent_id: int,
    opponent_elo: float | None,
    opponent_surface_elo: float | None,
    expected_win_prob: float | None,
    games_won: int,
    games_lost: int,
    sets_won: int,
    sets_lost: int,
    tiebreaks_played: int = 0,
    tiebreaks_won: int = 0,
    deciding_set_played: bool = False,
    straight_sets: bool = False,
    close_match: bool = False,
    tournament_id: int = 77,
    tournament_edition_id: int = 101,
    surface: str = "Clay",
    level_code: str = "A",
) -> MatchRecord:
    return MatchRecord(
        temporal_order=temporal_order,
        won=won,
        surface=surface,
        level_code=level_code,
        games_won=games_won,
        games_lost=games_lost,
        tournament_edition_id=tournament_edition_id,
        tournament_id=tournament_id,
        match_date=match_date,
        opponent_id=opponent_id,
        opponent_elo=opponent_elo,
        opponent_surface_elo=opponent_surface_elo,
        expected_win_prob=expected_win_prob,
        sets_won=sets_won,
        sets_lost=sets_lost,
        tiebreaks_played=tiebreaks_played,
        tiebreaks_won=tiebreaks_won,
        deciding_set_played=deciding_set_played,
        straight_sets=straight_sets,
        close_match=close_match,
    )


def _ctx() -> MatchContext:
    return MatchContext(
        match_id=9001,
        match_date=date(2026, 3, 1),
        surface="Clay",
        level_code="A",
        tour="ATP",
        gender="men",
        round="QF",
        year=2026,
        seed_a=None,
        seed_b=None,
        temporal_order=2026030100000010,
        tournament_edition_id=999,
        tournament_id=77,
        match_date_estimated=False,
    )


def test_opponent_quality_features_use_prior_matches_only() -> None:
    from teelo.features.groups.opponent_quality import OpponentQualityFeatures

    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)

    state_a.update(
        _record(
            temporal_order=2026020100000010,
            won=True,
            match_date=date(2026, 2, 1),
            opponent_id=10,
            opponent_elo=1600.0,
            opponent_surface_elo=1580.0,
            expected_win_prob=0.40,
            games_won=12,
            games_lost=8,
            sets_won=2,
            sets_lost=0,
            straight_sets=True,
        ),
        elo_post=1520.0,
        surface_elo_post=1510.0,
    )
    state_a.update(
        _record(
            temporal_order=2026021000000010,
            won=False,
            match_date=date(2026, 2, 10),
            opponent_id=11,
            opponent_elo=1450.0,
            opponent_surface_elo=1460.0,
            expected_win_prob=0.65,
            games_won=9,
            games_lost=13,
            sets_won=1,
            sets_lost=2,
            deciding_set_played=True,
            close_match=True,
        ),
        elo_post=1508.0,
        surface_elo_post=1504.0,
    )

    features = OpponentQualityFeatures().compute(state_a, state_b, _ctx())

    assert features["opp_elo_avg_8_a"] == 1525.0
    assert features["opp_surface_elo_avg_8_a"] == 1520.0
    assert features["wins_vs_higher_elo_8_a"] == 1.0
    assert features["losses_vs_lower_elo_8_a"] == 1.0
    assert features["elo_overperf_8_a"] == -0.025000000000000022
    assert features["opp_elo_avg_8_b"] is None


def test_dominance_features_keep_rates_missing_without_sample() -> None:
    from teelo.features.groups.dominance import DominanceFeatures

    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)

    state_a.update(
        _record(
            temporal_order=2026020100000010,
            won=True,
            match_date=date(2026, 2, 1),
            opponent_id=10,
            opponent_elo=1600.0,
            opponent_surface_elo=1580.0,
            expected_win_prob=0.40,
            games_won=12,
            games_lost=8,
            sets_won=2,
            sets_lost=0,
            tiebreaks_played=1,
            tiebreaks_won=1,
            straight_sets=True,
        ),
        elo_post=1520.0,
        surface_elo_post=1510.0,
    )

    features = DominanceFeatures().compute(state_a, state_b, _ctx())

    assert features["game_diff_avg_8_a"] == 4.0
    assert features["set_diff_avg_8_a"] == 2.0
    assert features["straight_sets_rate_8_a"] is None
    assert features["tiebreak_rate_8_a"] is None
    assert features["game_diff_avg_8_b"] is None


def test_dominance_features_compute_rates_with_enough_matches() -> None:
    from teelo.features.groups.dominance import DominanceFeatures

    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)

    for idx, won in enumerate((True, True, False), start=1):
        state_a.update(
            _record(
                temporal_order=2026020000000010 + idx,
                won=won,
                match_date=date(2026, 2, idx),
                opponent_id=20 + idx,
                opponent_elo=1500.0 + idx,
                opponent_surface_elo=1490.0 + idx,
                expected_win_prob=0.5,
                games_won=12 if won else 10,
                games_lost=8 if won else 12,
                sets_won=2 if won else 1,
                sets_lost=0 if won else 2,
                tiebreaks_played=1,
                tiebreaks_won=1 if won else 0,
                deciding_set_played=not won,
                straight_sets=won,
                close_match=not won,
            ),
            elo_post=1510.0 + idx,
            surface_elo_post=1505.0 + idx,
        )

    features = DominanceFeatures().compute(state_a, state_b, _ctx())

    assert round(features["straight_sets_rate_8_a"], 4) == 0.6667
    assert round(features["deciding_set_rate_8_a"], 4) == 0.3333
    assert features["tiebreak_rate_8_a"] == 1.0
    assert round(features["tiebreak_win_rate_8_a"], 4) == 0.6667
    assert round(features["close_match_rate_8_a"], 4) == 0.3333
