"""Tests for hourly orchestrator stage registration defaults."""

from scripts.run_hourly_update import _build_registry


def test_hourly_registry_defaults():
    registry = _build_registry()

    defaults = registry.default_stage_names()
    assert "current_events_ingest" in defaults
    assert "elo_incremental" in defaults
    assert "player_enrichment_incremental" not in defaults

    player_stage = registry.get("player_enrichment_incremental")
    assert player_stage.enabled_by_default is False
