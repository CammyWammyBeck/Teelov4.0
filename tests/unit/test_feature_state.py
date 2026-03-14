from datetime import date

from teelo.features.state import MatchRecord, PlayerState


def test_player_state_update_tracks_new_match_summaries() -> None:
    state = PlayerState(player_id=11)

    record = MatchRecord(
        temporal_order=2026010100000010,
        won=True,
        surface="Clay",
        level_code="A",
        games_won=12,
        games_lost=7,
        tournament_edition_id=101,
        tournament_id=77,
        match_date=date(2026, 1, 1),
        opponent_id=29,
        opponent_elo=1610.0,
        opponent_surface_elo=1585.0,
        expected_win_prob=0.42,
        sets_won=2,
        sets_lost=0,
        tiebreaks_played=1,
        tiebreaks_won=1,
        deciding_set_played=False,
        straight_sets=True,
        close_match=False,
    )

    state.update(record, elo_post=1524.0, surface_elo_post=1512.0)

    last_record = state.matches[-1]
    assert last_record.opponent_elo == 1610.0
    assert last_record.expected_win_prob == 0.42
    assert last_record.sets_won == 2
    assert last_record.tiebreaks_played == 1
    assert last_record.straight_sets is True


def test_player_state_update_tracks_tournament_history_counts() -> None:
    state = PlayerState(player_id=11)

    first = MatchRecord(
        temporal_order=2026010100000010,
        won=True,
        surface="Hard",
        level_code="A",
        games_won=13,
        games_lost=10,
        tournament_edition_id=201,
        tournament_id=55,
        match_date=date(2026, 1, 5),
        opponent_id=30,
        opponent_elo=1500.0,
        opponent_surface_elo=1500.0,
        expected_win_prob=0.50,
        sets_won=2,
        sets_lost=1,
        tiebreaks_played=0,
        tiebreaks_won=0,
        deciding_set_played=True,
        straight_sets=False,
        close_match=True,
    )
    second = first._replace(
        temporal_order=2026010200000010,
        won=False,
        tournament_edition_id=202,
        match_date=date(2026, 1, 6),
        opponent_id=31,
    )

    state.update(first, elo_post=1512.0, surface_elo_post=1510.0)
    state.update(second, elo_post=1502.0, surface_elo_post=1504.0)

    assert state.tournament_matches[55] == 2
    assert state.tournament_wins[55] == 1
    assert state.tournament_losses[55] == 1


def test_surface_elo_state_distinguishes_default_from_observed_surface() -> None:
    fresh = PlayerState(player_id=99)
    assert fresh.has_observed_surface_elo("Grass") is False

    record = MatchRecord(
        temporal_order=2026070100000010,
        won=True,
        surface="Grass",
        level_code="G",
        games_won=18,
        games_lost=14,
        tournament_edition_id=301,
        tournament_id=88,
        match_date=date(2026, 7, 1),
        opponent_id=45,
        opponent_elo=1700.0,
        opponent_surface_elo=1715.0,
        expected_win_prob=0.46,
        sets_won=3,
        sets_lost=1,
        tiebreaks_played=2,
        tiebreaks_won=1,
        deciding_set_played=False,
        straight_sets=False,
        close_match=True,
    )

    fresh.update(record, elo_post=1535.0, surface_elo_post=1540.0)

    assert fresh.has_observed_surface_elo("Grass") is True
