"""Tests for model artifact / version resolution helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from teelo.ml import versioning
from teelo.ml.versioning import (
    load_model_metadata_by_artifact,
    resolve_model_artifact_name,
)


def _write_meta(dir_path: Path, artifact: str, meta: dict) -> None:
    (dir_path / f"{artifact}_meta.json").write_text(json.dumps(meta))


def test_resolve_accepts_artifact_name_directly() -> None:
    assert resolve_model_artifact_name("prediction_v17.json") == "prediction_v17.json"


def test_resolve_returns_none_for_missing_input() -> None:
    assert resolve_model_artifact_name(None) is None
    assert resolve_model_artifact_name("") is None


def test_resolve_returns_none_when_unknown_legacy_ref(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(versioning, "list_models", lambda: [])
    assert resolve_model_artifact_name("not-an-artifact", models_dir=tmp_path) is None


def test_resolve_maps_legacy_timestamp_to_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(versioning, "list_models", lambda: [])
    _write_meta(
        tmp_path,
        "prediction_v9.json",
        {"created_at": "2026-01-15T00:00:00+00:00", "feature_names": ["a"]},
    )
    _write_meta(
        tmp_path,
        "prediction_v10.json",
        {"created_at": "2026-02-20T12:34:56+00:00", "feature_names": ["a"]},
    )

    assert (
        resolve_model_artifact_name("2026-02-20T12:34:56+00:00", models_dir=tmp_path)
        == "prediction_v10.json"
    )


def test_load_metadata_returns_none_when_missing(tmp_path: Path) -> None:
    assert load_model_metadata_by_artifact("nope.json", models_dir=tmp_path) is None


def test_load_metadata_reads_json(tmp_path: Path) -> None:
    _write_meta(tmp_path, "prediction_v3.json", {"model_artifact": "prediction_v3.json"})
    meta = load_model_metadata_by_artifact("prediction_v3.json", models_dir=tmp_path)
    assert meta == {"model_artifact": "prediction_v3.json"}


def test_trainer_metadata_includes_model_artifact_and_feature_set() -> None:
    """Guard the contract that trainer metadata exposes the fields the
    explanation pipeline relies on. Validated by reading the source rather
    than training a full model (which needs the production DB)."""
    source = Path(__file__).resolve().parents[2] / "src" / "teelo" / "ml" / "trainer.py"
    text = source.read_text()
    assert '"model_artifact": output.name' in text
    assert '"feature_set_name": self.feature_set_name' in text
