from __future__ import annotations

from datetime import date

from teelo.scrape.pipeline import TaskParams


def test_results_path_commits_before_cleanup_and_after_checkpoint():
    path = "/home/cammybeck/Teelov4.0/src/teelo/scrape/pipeline.py"
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()

    expected = """stats = ingest_results(session, matches, edition, identity_service)
                    results_ingest_stats = stats  # captured for inline ELO update below
                    session.commit()"""
    assert expected in src

    expected_cleanup = """cancelled = cancel_stale_pending_matches(session, edition)
        session.commit()"""
    assert expected_cleanup in src


def test_task_params_still_constructible_for_all_tour_families():
    samples = [
        TaskParams(
            tournament_id="rouen",
            year=2026,
            tour_key="WTA",
            tournament_number="2066",
        ),
        TaskParams(
            tournament_id="barcelona",
            year=2026,
            tour_key="ATP",
            tournament_number="425",
        ),
        TaskParams(
            tournament_id="m15-somewhere",
            year=2026,
            tour_key="ITF_MEN",
            tournament_url="https://example.com",
        ),
    ]
    assert [s.tour_key for s in samples] == ["WTA", "ATP", "ITF_MEN"]
    assert all(s.year == 2026 for s in samples)
