from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

from teelo.scrape.base import ScrapedFixture
from teelo.scrape.pipeline import _schedule_needs_ingest_for_missing_db_data


def _fixture(**overrides):
    values = {
        "tournament_name": "Roland Garros",
        "tournament_id": "roland-garros",
        "tournament_year": 2026,
        "tournament_level": "Grand Slam",
        "tournament_surface": "Clay",
        "round": "R128",
        "scheduled_date": "2026-05-24",
        "scheduled_time": "11:00",
        "court": "Court 6",
        "player_a_name": "Player A",
        "player_a_external_id": "320983",
        "player_b_name": "Player B",
        "player_b_external_id": "322280",
        "source": "wta",
    }
    values.update(overrides)
    return ScrapedFixture(**values)


def _match(**overrides):
    values = {
        "external_id": "2026_roland-garros_R128_320983_322280",
        "scheduled_date": None,
        "scheduled_datetime": None,
        "court": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_schedule_checkpoint_guard_blocks_skip_when_db_schedule_missing():
    assert _schedule_needs_ingest_for_missing_db_data([_fixture()], [_match()]) is True


def test_schedule_checkpoint_guard_allows_skip_when_db_schedule_matches():
    match = _match(
        scheduled_date=date(2026, 5, 24),
        scheduled_datetime=datetime(2026, 5, 24, 11, 0),
        court="Court 6",
    )

    assert _schedule_needs_ingest_for_missing_db_data([_fixture()], [match]) is False


def test_schedule_checkpoint_guard_ignores_unscheduled_fixture():
    fixture = _fixture(scheduled_date=None, scheduled_time=None, court=None)

    assert _schedule_needs_ingest_for_missing_db_data([fixture], []) is False
