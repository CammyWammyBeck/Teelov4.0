from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import select

from teelo.db.models import FeatureSet
from teelo.db.session import get_session
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
