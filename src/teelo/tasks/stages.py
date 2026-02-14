"""Stage registry primitives for pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from teelo.tasks.runtime import StageContext, StageResult

StageRunner = Callable[[StageContext], StageResult | Awaitable[StageResult]]


@dataclass(frozen=True)
class StageDefinition:
    """Registered stage metadata and runner implementation."""

    name: str
    runner: StageRunner
    description: str = ""
    enabled_by_default: bool = True
    timeout_seconds: int | None = None


class StageRegistry:
    """In-memory registry for named update stages."""

    def __init__(self) -> None:
        self._stages: dict[str, StageDefinition] = {}

    def register(self, stage: StageDefinition) -> None:
        if stage.name in self._stages:
            raise ValueError(f"Stage already registered: {stage.name}")
        self._stages[stage.name] = stage

    def get(self, stage_name: str) -> StageDefinition:
        try:
            return self._stages[stage_name]
        except KeyError as exc:
            raise KeyError(f"Unknown stage: {stage_name}") from exc

    def default_stage_names(self) -> list[str]:
        return [name for name, stage in self._stages.items() if stage.enabled_by_default]

    def resolve(
        self,
        include: list[str] | None = None,
        skip: set[str] | None = None,
    ) -> list[StageDefinition]:
        names = include or self.default_stage_names()
        skipped = skip or set()
        return [self.get(name) for name in names if name not in skipped]
