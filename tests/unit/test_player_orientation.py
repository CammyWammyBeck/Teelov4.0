"""Tests for the 'never swap player_a/player_b' orientation-preserving behavior."""

from datetime import date
from unittest.mock import MagicMock

from teelo.db.models import Match
from teelo.scrape.base import ScrapedMatch
from teelo.services.results_ingestion import (
    _flip_score_structured,
    _update_match_with_result,
)
from teelo.services.draw_ingestion import (
    _flip_score_structured as draw_flip_score_structured,
)


# ── _flip_score_structured ──────────────────────────────────────────


def test_flip_score_structured_normal():
    score = [{"a": 6, "b": 4}, {"a": 6, "b": 3}]
    flipped = _flip_score_structured(score)
    assert flipped == [{"a": 4, "b": 6}, {"a": 3, "b": 6}]


def test_flip_score_structured_tiebreak():
    score = [{"a": 6, "b": 4}, {"a": 7, "b": 6, "tb_a": 7, "tb_b": 5}]
    flipped = _flip_score_structured(score)
    assert flipped == [{"a": 4, "b": 6}, {"a": 6, "b": 7, "tb_a": 5, "tb_b": 7}]


def test_flip_score_structured_retirement():
    score = [{"a": 6, "b": 4}, {"a": 2, "b": 1, "retired": True}]
    flipped = _flip_score_structured(score)
    assert flipped == [{"a": 4, "b": 6}, {"a": 1, "b": 2, "retired": True}]


def test_flip_score_structured_none():
    assert _flip_score_structured(None) is None


def test_flip_score_structured_empty():
    assert _flip_score_structured([]) == []


def test_flip_score_structured_draw_module_matches():
    """Both modules define identical _flip_score_structured."""
    score = [{"a": 6, "b": 4, "tb_a": 7, "tb_b": 3}]
    assert _flip_score_structured(score) == draw_flip_score_structured(score)


# ── _update_match_with_result — reversed order ─────────────────────


def _make_edition():
    edition = MagicMock()
    edition.start_date = date(2026, 3, 1)
    edition.end_date = date(2026, 3, 14)
    return edition


def _make_scraped(
    player_a_name="Player One",
    player_b_name="Player Two",
    winner_name="Player Two",
    score_raw="6-4 6-3",
    player_a_seed=None,
    player_b_seed=None,
):
    return ScrapedMatch(
        external_id="test_123",
        source="atp",
        tournament_name="Test Open",
        tournament_id="test",
        tournament_year=2026,
        tournament_level="ATP 250",
        tournament_surface="Hard",
        round="QF",
        player_a_name=player_a_name,
        player_b_name=player_b_name,
        winner_name=winner_name,
        score_raw=score_raw,
        status="completed",
        player_a_seed=player_a_seed,
        player_b_seed=player_b_seed,
    )


def test_reversed_order_preserves_player_ids():
    """When results scraper has reversed A/B, match keeps original orientation."""
    match = Match(player_a_id=1, player_b_id=2, prediction_a=0.65)

    _update_match_with_result(
        match=match,
        scraped=_make_scraped(),
        edition=_make_edition(),
        player_a_id=2,  # scraped A maps to match's B
        player_b_id=1,  # scraped B maps to match's A
        score_structured=[{"a": 6, "b": 4}, {"a": 6, "b": 3}],
        match_date=date(2026, 3, 10),
        match_date_estimated=False,
    )

    # Player orientation stays the same
    assert match.player_a_id == 1
    assert match.player_b_id == 2
    # Prediction is preserved (no clearing!)
    assert match.prediction_a == 0.65


def test_reversed_order_flips_score_structured():
    """Score structured is flipped to match existing orientation."""
    match = Match(player_a_id=1, player_b_id=2)

    _update_match_with_result(
        match=match,
        scraped=_make_scraped(),
        edition=_make_edition(),
        player_a_id=2,
        player_b_id=1,
        score_structured=[{"a": 6, "b": 4}, {"a": 6, "b": 3}],
        match_date=None,
        match_date_estimated=False,
    )

    # score_structured flipped: match's player_a (id=1) had 4 and 3
    assert match.score_structured == [{"a": 4, "b": 6}, {"a": 3, "b": 6}]


def test_reversed_order_maps_seeds():
    """Seeds from reversed scraper are mapped to correct player slots."""
    match = Match(player_a_id=1, player_b_id=2)

    scraped = _make_scraped(player_a_seed="3", player_b_seed="7")

    _update_match_with_result(
        match=match,
        scraped=scraped,
        edition=_make_edition(),
        player_a_id=2,
        player_b_id=1,
        score_structured=[{"a": 6, "b": 4}],
        match_date=None,
        match_date_estimated=False,
    )

    # Scraped A (seed 3) maps to match's B; scraped B (seed 7) maps to match's A
    assert match.player_a_seed == "7"
    assert match.player_b_seed == "3"


def test_reversed_order_winner_id_correct():
    """Winner ID is a player ID — correct regardless of orientation."""
    match = Match(player_a_id=1, player_b_id=2)

    # Scraped: player_a_name="Player Two" (id=2), winner_name="Player Two"
    scraped = _make_scraped(
        player_a_name="Player Two",
        player_b_name="Player One",
        winner_name="Player Two",
    )

    _update_match_with_result(
        match=match,
        scraped=scraped,
        edition=_make_edition(),
        player_a_id=2,  # "Player Two" resolved to id=2
        player_b_id=1,  # "Player One" resolved to id=1
        score_structured=[{"a": 6, "b": 4}],
        match_date=None,
        match_date_estimated=False,
    )

    # Winner is player 2 regardless of orientation
    assert match.winner_id == 2


# ── _update_match_with_result — same order ─────────────────────────


def test_same_order_no_flipping():
    """When scraper order matches, everything maps 1:1."""
    match = Match(player_a_id=1, player_b_id=2, prediction_a=0.65)

    scraped = _make_scraped(player_a_seed="3", player_b_seed="7")

    _update_match_with_result(
        match=match,
        scraped=scraped,
        edition=_make_edition(),
        player_a_id=1,
        player_b_id=2,
        score_structured=[{"a": 6, "b": 4}],
        match_date=None,
        match_date_estimated=False,
    )

    assert match.player_a_id == 1
    assert match.player_b_id == 2
    assert match.prediction_a == 0.65
    assert match.score_structured == [{"a": 6, "b": 4}]
    assert match.player_a_seed == "3"
    assert match.player_b_seed == "7"


# ── _update_match_with_result — match with no players yet ──────────


def test_no_existing_players_adopts_incoming():
    """If match has no players, adopt the incoming orientation."""
    match = Match(player_a_id=None, player_b_id=None)

    _update_match_with_result(
        match=match,
        scraped=_make_scraped(),
        edition=_make_edition(),
        player_a_id=1,
        player_b_id=2,
        score_structured=[{"a": 6, "b": 4}],
        match_date=None,
        match_date_estimated=False,
    )

    assert match.player_a_id == 1
    assert match.player_b_id == 2
    assert match.score_structured == [{"a": 6, "b": 4}]
