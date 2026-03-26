"""ELO data lookup for feature computation.

Provides read-only access to ELO values computed by the ELO updater.
Replaces the previous approach of storing ELO in PlayerState.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EloLookup:
    """Read-only ELO data sourced from the ELO updater's persisted tables."""

    elo: dict[int, float] = field(default_factory=dict)
    surface_elo: dict[int, dict[str, float]] = field(default_factory=dict)
    elo_peak: dict[int, float] = field(default_factory=dict)
    surface_elo_peak: dict[int, dict[str, float]] = field(default_factory=dict)
    elo_history: dict[int, list[tuple[int, float]]] = field(default_factory=dict)

    def get_elo(self, player_id: int) -> float:
        return self.elo.get(player_id, 1500.0)

    def get_surface_elo(self, player_id: int, surface: str) -> float:
        return self.surface_elo.get(player_id, {}).get(surface, 1500.0)

    def get_elo_peak(self, player_id: int) -> float:
        return self.elo_peak.get(player_id, 1500.0)

    def get_surface_elo_peak(self, player_id: int, surface: str) -> float:
        return self.surface_elo_peak.get(player_id, {}).get(surface, 1500.0)

    def get_surface_elos(self, player_id: int) -> dict[str, float]:
        return self.surface_elo.get(player_id, {})

    def get_elo_history(self, player_id: int) -> list[tuple[int, float]]:
        return self.elo_history.get(player_id, [])
