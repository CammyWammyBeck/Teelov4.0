"""Checkpoint persistence helpers for incremental pipeline stages."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from teelo.db.models import PipelineCheckpoint


class DBCheckpointStore:
    """Simple key/value checkpoint store backed by the database."""

    def __init__(self, session: Session):
        self.session = session

    def get(self, key: str) -> dict[str, Any] | None:
        row = self.session.get(PipelineCheckpoint, key)
        if row is None:
            return None
        return row.value_json

    def set(self, key: str, value: dict[str, Any]) -> None:
        row = self.session.get(PipelineCheckpoint, key)
        if row is None:
            row = PipelineCheckpoint(
                key=key,
                value_json=value,
                updated_at=datetime.utcnow(),
            )
            self.session.add(row)
        else:
            row.value_json = value
            row.updated_at = datetime.utcnow()

        self.session.flush()
