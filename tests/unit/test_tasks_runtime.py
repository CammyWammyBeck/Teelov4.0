"""Unit tests for task runtime primitives."""

from datetime import datetime, timedelta

import pytest

from teelo.tasks.locks import advisory_lock_key
from teelo.tasks.runtime import StageResult
from teelo.tasks.stages import StageDefinition, StageRegistry


def _noop_stage(_ctx):
    raise NotImplementedError


def test_stage_registry_register_and_get():
    registry = StageRegistry()
    stage = StageDefinition(name="alpha", runner=_noop_stage)
    registry.register(stage)

    loaded = registry.get("alpha")
    assert loaded.name == "alpha"
    assert loaded.runner is _noop_stage


def test_stage_registry_duplicate_registration_raises():
    registry = StageRegistry()
    stage = StageDefinition(name="dup", runner=_noop_stage)
    registry.register(stage)
    with pytest.raises(ValueError):
        registry.register(stage)


def test_stage_registry_resolve_default_and_skip():
    registry = StageRegistry()
    registry.register(StageDefinition(name="a", runner=_noop_stage, enabled_by_default=True))
    registry.register(StageDefinition(name="b", runner=_noop_stage, enabled_by_default=True))
    registry.register(StageDefinition(name="c", runner=_noop_stage, enabled_by_default=False))

    default_names = [s.name for s in registry.resolve()]
    assert default_names == ["a", "b"]

    include_names = [s.name for s in registry.resolve(include=["c", "a"], skip={"a"})]
    assert include_names == ["c"]


def test_stage_result_duration_and_payload():
    started = datetime(2026, 2, 14, 10, 0, 0)
    ended = started + timedelta(seconds=12.5)
    result = StageResult(
        stage_name="example",
        status="success",
        started_at=started,
        ended_at=ended,
        metrics={"rows": 123},
    )

    assert result.duration_s == 12.5
    payload = result.to_dict()
    assert payload["stage_name"] == "example"
    assert payload["status"] == "success"
    assert payload["duration_s"] == 12.5
    assert payload["metrics"] == {"rows": 123}


def test_advisory_lock_key_is_stable_64bit_int():
    key_a1 = advisory_lock_key("teelo_hourly_pipeline")
    key_a2 = advisory_lock_key("teelo_hourly_pipeline")
    key_b = advisory_lock_key("other_pipeline")

    assert isinstance(key_a1, int)
    assert key_a1 == key_a2
    assert key_a1 != key_b
