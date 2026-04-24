from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy import select

from teelo.db.models import FeatureSet
from teelo.db.session import get_session
from teelo.features import default_preset_for_feature_set
from teelo.storage import list_models

_PATTERN = re.compile(r"^prediction_v(\d+)\.json$")


def _versions() -> list[int]:
    versions: list[int] = []
    for name in list_models():
        match = _PATTERN.match(name)
        if match:
            versions.append(int(match.group(1)))
    return sorted(versions)


def next_model_path(models_dir: Path = Path("models")) -> str:
    versions = _versions()
    next_version = versions[-1] + 1 if versions else 1
    return str(models_dir / f"prediction_v{next_version}.json")


def latest_model_path(models_dir: Path = Path("models")) -> str:
    versions = _versions()
    if not versions:
        raise FileNotFoundError("No model files found in S3 matching prediction_v{N}.json")
    return str(models_dir / f"prediction_v{versions[-1]}.json")


def latest_feature_set() -> str:
    with get_session() as session:
        fs = session.execute(
            select(FeatureSet.name).order_by(FeatureSet.created_at.desc()).limit(1)
        ).scalar_one_or_none()
    if fs is None:
        raise ValueError("No feature sets found in database")
    return fs


def latest_preset(feature_set_name: str) -> str:
    """Map a feature_set_name to the appropriate feature-engine preset.

    Forecast uses feature presets ("full", "trimmed_v3", etc.) to build the same
    registry used in training/inference.
    """

    return default_preset_for_feature_set(feature_set_name)


def load_model_metadata_by_artifact(
    model_artifact: str, models_dir: Path = Path("models")
) -> dict[str, Any] | None:
    """Load the metadata JSON for a given artifact name from the local models dir.

    Returns None if the metadata file is not present locally. Callers that need
    to force a download should call teelo.storage.download_model() first.
    """
    meta_path = models_dir / f"{model_artifact}_meta.json"
    if not meta_path.exists():
        return None
    with meta_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        return None
    return data


def _build_legacy_resolution_index(models_dir: Path) -> dict[str, str]:
    """Scan model metadata files and build a created_at -> artifact map.

    Used to resolve legacy prediction_model_version values (ISO timestamps) back
    to the artifact filename they came from.
    """
    index: dict[str, str] = {}
    artifacts: list[str] = []
    try:
        artifacts.extend(list_models())
    except Exception:
        pass
    if models_dir.is_dir():
        for meta_path in models_dir.glob("*_meta.json"):
            artifact_name = meta_path.name[: -len("_meta.json")]
            if artifact_name not in artifacts:
                artifacts.append(artifact_name)
    for artifact_name in artifacts:
        meta = load_model_metadata_by_artifact(artifact_name, models_dir)
        if meta is None:
            continue
        created_at = meta.get("created_at")
        if isinstance(created_at, str):
            index[created_at] = artifact_name
    return index


@lru_cache(maxsize=1)
def _cached_legacy_index() -> dict[str, str]:
    return _build_legacy_resolution_index(Path("models"))


def resolve_model_artifact_name(
    model_ref: str | None, models_dir: Path | None = None
) -> str | None:
    """Map a prediction_model_version value to an artifact filename.

    New predictions store the artifact name directly (e.g. 'prediction_v17.json').
    Legacy predictions stored the metadata 'created_at' timestamp; this helper
    falls back to a metadata scan for those.

    Returns None when the reference cannot be resolved.
    """
    if not model_ref:
        return None
    if _PATTERN.match(model_ref):
        return model_ref
    if models_dir is not None:
        return _build_legacy_resolution_index(models_dir).get(model_ref)
    return _cached_legacy_index().get(model_ref)


def infer_feature_set_name(feature_names: list[str]) -> str:
    """Pick the FeatureSet whose feature_definitions keys exactly match the given list.

    Raises when there is no match or more than one match, to avoid silently
    attributing a prediction to the wrong feature set.
    """
    target = set(feature_names)
    with get_session() as session:
        rows = session.execute(select(FeatureSet)).scalars().all()
    matches = [fs.name for fs in rows if set(fs.feature_definitions.keys()) == target]
    if not matches:
        raise ValueError(
            "No FeatureSet has feature_definitions matching the given feature names"
        )
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous feature set for given feature names: {matches}"
        )
    return matches[0]
