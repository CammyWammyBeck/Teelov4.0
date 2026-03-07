"""Feature group interfaces and registry."""

from __future__ import annotations

from abc import ABC, abstractmethod

from teelo.features.state import MatchContext, PlayerState


class FeatureGroup(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def feature_names(self) -> list[str]: ...

    @abstractmethod
    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]: ...


class FeatureRegistry:
    def __init__(self) -> None:
        self._groups: list[FeatureGroup] = []

    def register(self, group: FeatureGroup) -> None:
        self._groups.append(group)

    def all_feature_names(self) -> list[str]:
        names: list[str] = []
        for group in self._groups:
            names.extend(group.feature_names())
        return names

    def compute_all(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}
        for group in self._groups:
            features.update(group.compute(state_a, state_b, ctx))
        return features
