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


def test_fatigue_features_expose_player_differentials() -> None:
    from teelo.features.groups.fatigue import FatigueFeatures

    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)

    state_a.update(
        _record(
            temporal_order=2026022500000010,
            won=True,
            match_date=date(2026, 2, 25),
            opponent_id=41,
            opponent_elo=1500.0,
            opponent_surface_elo=1500.0,
            expected_win_prob=0.55,
            games_won=12,
            games_lost=9,
            sets_won=2,
            sets_lost=1,
            deciding_set_played=True,
            close_match=True,
        ),
        elo_post=1510.0,
        surface_elo_post=1508.0,
    )
    state_a.update(
        _record(
            temporal_order=2026022700000010,
            won=True,
            match_date=date(2026, 2, 27),
            opponent_id=42,
            opponent_elo=1510.0,
            opponent_surface_elo=1506.0,
            expected_win_prob=0.54,
            games_won=12,
            games_lost=7,
            sets_won=2,
            sets_lost=0,
            straight_sets=True,
            tournament_edition_id=999,
        ),
        elo_post=1518.0,
        surface_elo_post=1511.0,
    )
    state_b.update(
        _record(
            temporal_order=2026022800000010,
            won=False,
            match_date=date(2026, 2, 28),
            opponent_id=51,
            opponent_elo=1520.0,
            opponent_surface_elo=1518.0,
            expected_win_prob=0.48,
            games_won=10,
            games_lost=12,
            sets_won=1,
            sets_lost=2,
            tournament_edition_id=999,
        ),
        elo_post=1498.0,
        surface_elo_post=1495.0,
    )

    features = FatigueFeatures().compute(state_a, state_b, _ctx())

    assert features["rest_days_a"] == 2.0
    assert features["rest_days_b"] == 1.0
    assert features["rest_days_diff_ab"] == 1.0
    assert features["matches_7d_diff_ab"] == 1.0
    assert features["games_7d_diff_ab"] == 18.0
    assert features["games_this_tournament_diff_ab"] == -3.0


def test_tournament_history_features_use_only_prior_tournament_history() -> None:
    from teelo.features.groups.tournament_history import TournamentHistoryFeatures

    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)

    state_a.update(
        _record(
            temporal_order=2025020100000010,
            won=True,
            match_date=date(2025, 2, 1),
            opponent_id=61,
            opponent_elo=1500.0,
            opponent_surface_elo=1490.0,
            expected_win_prob=0.52,
            games_won=12,
            games_lost=8,
            sets_won=2,
            sets_lost=0,
            tournament_id=77,
            tournament_edition_id=700,
        ),
        elo_post=1512.0,
        surface_elo_post=1509.0,
    )
    state_a.update(
        _record(
            temporal_order=2024080100000010,
            won=False,
            match_date=date(2024, 8, 1),
            opponent_id=62,
            opponent_elo=1520.0,
            opponent_surface_elo=1510.0,
            expected_win_prob=0.49,
            games_won=9,
            games_lost=12,
            sets_won=0,
            sets_lost=2,
            tournament_id=12,
            tournament_edition_id=701,
        ),
        elo_post=1498.0,
        surface_elo_post=1497.0,
    )

    features = TournamentHistoryFeatures().compute(state_a, state_b, _ctx())

    assert features["tournament_match_count_a"] == 1.0
    assert features["tournament_match_count_b"] == 0.0
    assert features["tournament_win_rate_a"] is None
    assert features["tournament_win_rate_b"] is None


def test_confidence_features_emit_numeric_companions_for_sparse_signals() -> None:
    from teelo.features.groups.confidence import ConfidenceFeatures

    state_a = PlayerState(player_id=1)
    state_b = PlayerState(player_id=2)

    state_a.update(
        _record(
            temporal_order=2026022500000010,
            won=True,
            match_date=date(2026, 2, 25),
            opponent_id=71,
            opponent_elo=1550.0,
            opponent_surface_elo=1540.0,
            expected_win_prob=0.46,
            games_won=12,
            games_lost=9,
            sets_won=2,
            sets_lost=1,
            tournament_id=77,
            tournament_edition_id=998,
        ),
        elo_post=1510.0,
        surface_elo_post=1507.0,
    )
    state_a.h2h[2].append(
        state_a.h2h[71][0]._replace(surface="Clay", match_date=date(2026, 1, 10))
    )

    estimated_ctx = _ctx()
    estimated_ctx.match_date_estimated = True

    features = ConfidenceFeatures().compute(state_a, state_b, estimated_ctx)

    assert features["surface_elo_observed_a"] == 1.0
    assert features["surface_elo_default_b"] == 1.0
    assert features["surface_elo_match_count_a"] == 1.0
    assert features["opponent_quality_sample_count_8_a"] == 1.0
    assert features["tournament_history_sample_count_a"] == 1.0
    assert features["h2h_sample_count"] == 1.0
    assert features["h2h_surface_sample_count"] == 1.0
    assert features["match_date_estimated_flag"] == 1.0
