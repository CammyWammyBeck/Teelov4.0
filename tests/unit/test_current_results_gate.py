from __future__ import annotations

from datetime import date

from teelo.scrape.pipeline import TaskParams, _should_scrape_results


def test_results_gate_allows_pre_main_draw_qualifying_within_check_window():
    params = TaskParams(
        tournament_id="madrid",
        year=2026,
        tour_key="ATP",
        tournament_level="Masters 1000",
        start_date="2026-04-22",
        end_date="2026-05-03",
        tournament_number="1536",
    )
    should, reason = _should_scrape_results(params, date(2026, 4, 21), fast_mode=False, force=False)
    assert should is True
    assert reason == ""


def test_results_gate_still_skips_far_before_check_window():
    params = TaskParams(
        tournament_id="roland-garros",
        year=2026,
        tour_key="ATP",
        tournament_level="Grand Slam",
        start_date="2026-05-24",
        end_date="2026-06-07",
    )
    should, reason = _should_scrape_results(params, date(2026, 5, 10), fast_mode=False, force=False)
    assert should is False
    assert "check window" in reason


def test_results_gate_allows_non_slam_three_day_lead():
    params = TaskParams(
        tournament_id="rome",
        year=2026,
        tour_key="WTA",
        tournament_level="WTA 1000",
        start_date="2026-05-06",
        end_date="2026-05-17",
    )
    should, reason = _should_scrape_results(params, date(2026, 5, 3), fast_mode=False, force=False)
    assert should is True
    assert reason == ""
