from __future__ import annotations

from teelo.scrape.pipeline import _should_checkpoint_results
from teelo.services.results_ingestion import ResultsIngestionStats


def test_results_checkpoint_allowed_for_clean_ingest():
    stats = ResultsIngestionStats(
        total_matches=3,
        matches_updated=3,
        matches_skipped_duplicate=0,
        skipped_no_player_match=0,
        errors=[],
    )

    should_checkpoint, reason = _should_checkpoint_results(stats)

    assert should_checkpoint is True
    assert reason == ""


def test_results_checkpoint_blocked_when_player_resolution_skips_rows():
    stats = ResultsIngestionStats(
        total_matches=3,
        matches_updated=2,
        skipped_no_player_match=1,
        errors=[],
    )

    should_checkpoint, reason = _should_checkpoint_results(stats)

    assert should_checkpoint is False
    assert "unresolved players" in reason


def test_results_checkpoint_blocked_when_row_errors_occur():
    stats = ResultsIngestionStats(
        total_matches=3,
        matches_updated=2,
        skipped_no_player_match=0,
        errors=["synthetic row failure"],
    )

    should_checkpoint, reason = _should_checkpoint_results(stats)

    assert should_checkpoint is False
    assert "row errors" in reason


def test_pipeline_source_guards_checkpoint_write_after_results_ingest():
    path = "/home/cammybeck/Teelov4.0/src/teelo/scrape/pipeline.py"
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()

    assert "should_checkpoint, checkpoint_reason = _should_checkpoint_results(stats)" in src
    assert "if should_checkpoint:" in src
    assert "_write_checkpoint_fingerprint(session, results_key, results_fp, len(matches))" in src
    assert "Skipping results checkpoint for %s %s %s: %s" in src
