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

    @property
    def neutral_display(self) -> bool:
        """If True, feature values are not color-coded as advantage/disadvantage."""
        return False


class FeatureRegistry:
    def __init__(self, exclude: set[str] | None = None) -> None:
        self._groups: list[FeatureGroup] = []
        self._exclude: set[str] = exclude or set()

    def register(self, group: FeatureGroup) -> None:
        self._groups.append(group)

    def all_feature_names(self) -> list[str]:
        names: list[str] = []
        for group in self._groups:
            names.extend(n for n in group.feature_names() if n not in self._exclude)
        return names

    def compute_all(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}
        for group in self._groups:
            result = group.compute(state_a, state_b, ctx)
            if self._exclude:
                features.update({k: v for k, v in result.items() if k not in self._exclude})
            else:
                features.update(result)
        return features

    def grouped_features(self) -> dict[str, list[str]]:
        """Return {group_name: [feature_names]} for all registered groups."""
        result: dict[str, list[str]] = {}
        for group in self._groups:
            names = [n for n in group.feature_names() if n not in self._exclude]
            if names:
                result[group.name] = names
        return result

    def neutral_groups(self) -> set[str]:
        """Return set of group names where values should not be color-coded."""
        return {g.name for g in self._groups if g.neutral_display}
