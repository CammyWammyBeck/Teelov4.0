"""Shared runtime dataclasses for orchestrated update stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

StageStatus = Literal["success", "failed", "partial", "skipped"]


@dataclass(frozen=True)
class StageContext:
    """Runtime context passed to each stage handler."""

    run_id: str
    stage_name: str
    started_at: datetime
    artifacts_dir: Path
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class StageResult:
    """Normalized result returned by a stage handler."""

    stage_name: str
    status: StageStatus
    started_at: datetime
    ended_at: datetime
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @property
    def duration_s(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_s": self.duration_s,
            "metrics": self.metrics,
            "error": self.error,
        }
