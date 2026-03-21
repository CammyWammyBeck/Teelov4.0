from datetime import date
from teelo.features.state import MatchContext, MatchRecord, PlayerState


def test_match_record_new_fields_default() -> None:
    r = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
    )
    assert r.first_set_lost is False
    assert r.opponent_clutch_score is None
    assert r.opponent_specialist_score is None
    assert r.country_ioc is None


def test_match_record_new_fields_explicit() -> None:
    r = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        first_set_lost=True, opponent_clutch_score=0.52,
        opponent_specialist_score=45.0, country_ioc="GBR",
    )
    assert r.first_set_lost is True
    assert r.opponent_clutch_score == 0.52
    assert r.opponent_specialist_score == 45.0
    assert r.country_ioc == "GBR"


def test_match_context_new_fields() -> None:
    ctx = MatchContext(
        match_id=1, match_date=date(2026, 1, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1, tournament_id=1,
        tournament_country_ioc="GBR",
        player_a_nationality="ESP",
        player_b_nationality="GBR",
    )
    assert ctx.tournament_country_ioc == "GBR"
    assert ctx.player_a_nationality == "ESP"
    assert ctx.player_b_nationality == "GBR"


def test_match_context_new_fields_default() -> None:
    ctx = MatchContext(
        match_id=1, match_date=date(2026, 1, 1), surface="Hard",
        level_code="A", tour="ATP", gender="men", round="QF",
        year=2026, seed_a=None, seed_b=None, temporal_order=1,
        tournament_edition_id=1,
    )
    assert ctx.tournament_country_ioc is None
    assert ctx.player_a_nationality is None
    assert ctx.player_b_nationality is None


def test_player_state_new_fields() -> None:
    state = PlayerState(player_id=1)
    assert state.clutch_score is None
    assert state.country_record == {}
    assert state.region_record == {}


def test_player_state_update_tracks_country_record() -> None:
    state = PlayerState(player_id=1)
    record = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        country_ioc="GBR",
    )
    state.update(record, 1510.0, 1510.0)
    assert state.country_record["GBR"] == (1, 0)

    record2 = MatchRecord(
        temporal_order=2, won=False, surface="Hard", level_code="A",
        games_won=3, games_lost=6, tournament_edition_id=2,
        tournament_id=1, match_date=date(2026, 2, 1), opponent_id=3,
        country_ioc="GBR",
    )
    state.update(record2, 1505.0, 1505.0)
    assert state.country_record["GBR"] == (1, 1)


def test_player_state_update_tracks_region_record() -> None:
    state = PlayerState(player_id=1)
    record = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        country_ioc="GBR",
    )
    state.update(record, 1510.0, 1510.0)
    assert state.region_record["Europe"] == (1, 0)


def test_player_state_update_skips_country_when_none() -> None:
    state = PlayerState(player_id=1)
    record = MatchRecord(
        temporal_order=1, won=True, surface="Hard", level_code="A",
        games_won=6, games_lost=3, tournament_edition_id=1,
        tournament_id=1, match_date=date(2026, 1, 1), opponent_id=2,
        country_ioc=None,
    )
    state.update(record, 1510.0, 1510.0)
    assert state.country_record == {}
    assert state.region_record == {}
