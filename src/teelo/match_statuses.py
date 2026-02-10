"""Shared match-status definitions and helpers.

This module is the single source of truth for status groups that are reused
across web handlers, API defaults, and ingestion services.
"""

from __future__ import annotations

from typing import Iterable

# Individual statuses currently used in the system.
ALL_MATCH_STATUSES: tuple[str, ...] = (
    "upcoming",
    "scheduled",
    "completed",
    "retired",
    "walkover",
    "default",
    "cancelled",
)

# Canonical status groups.
MATCH_STATUS_GROUPS: dict[str, tuple[str, ...]] = {
    # Upcoming fixtures visible on home feed.
    "upcoming": ("upcoming", "scheduled"),
    # Finished outcomes we include by default in historical/result tables.
    "historical_default": ("completed", "retired", "walkover", "default"),
    # Statuses that represent matches no longer actionable.
    "terminal": ("completed", "retired", "walkover", "default", "cancelled"),
    # Statuses for matches still awaiting a result.
    "pending": ("upcoming", "scheduled"),
    # Full set used by UI filters when users want explicit control.
    "all": ALL_MATCH_STATUSES,
}


def get_status_group(group_name: str) -> tuple[str, ...]:
    """Return a named status group, raising KeyError for unknown names."""
    return MATCH_STATUS_GROUPS[group_name]


def normalize_status_filter(
    raw_statuses: Iterable[str] | None,
    *,
    default_group: str = "historical_default",
) -> list[str]:
    """Normalize requested statuses against known values.

    - If no statuses are provided, returns the statuses from ``default_group``.
    - Unknown statuses are ignored.
    - Order is preserved and duplicates are removed.
    """
    if raw_statuses is None:
        return list(get_status_group(default_group))

    seen: set[str] = set()
    normalized: list[str] = []

    for raw in raw_statuses:
        status = raw.strip().lower()
        if not status or status in seen or status not in ALL_MATCH_STATUSES:
            continue
        seen.add(status)
        normalized.append(status)

    if normalized:
        return normalized

    return list(get_status_group(default_group))
