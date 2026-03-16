"""Utilities for displaying ML features on the match detail page."""

from __future__ import annotations

import re
from typing import Any

# Special case display names for acronyms/abbreviations
_DISPLAY_OVERRIDES = {
    "elo": "ELO",
    "h2h": "H2H",
    "opp": "Opponent",
    "avg": "Avg",
    "var": "Variance",
    "perf": "Performance",
    "overperf": "Overperformance",
}

# Group key -> display name overrides
_GROUP_DISPLAY_NAMES = {
    "elo_core": "ELO Ratings",
    "elo_history": "ELO History",
    "elo_variance": "ELO Variance",
    "form": "Recent Form",
    "h2h": "Head to Head",
    "activity": "Activity",
    "dominance": "Match Dominance",
    "opponent_quality": "Opponent Quality",
    "tournament_history": "Tournament History",
    "fatigue": "Fatigue & Recovery",
    "confidence": "Data Confidence",
    "context": "Match Context",
}


def group_display_name(group_key: str) -> str:
    """Convert a feature group key to a human-readable name."""
    if group_key in _GROUP_DISPLAY_NAMES:
        return _GROUP_DISPLAY_NAMES[group_key]
    return group_key.replace("_", " ").title()


def feature_display_name(feature_key: str) -> str:
    """Convert a feature key to a human-readable name.

    Strips player markers (_a/_b suffix or _a_/_b_ infix), replaces
    underscores, applies special cases.

    Examples:
        'elo_a'          -> 'ELO'
        'win_rate_8w_a'  -> 'Win Rate 8w'
        'h2h_a_wins'     -> 'H2H Wins'
        'h2h_a_wins_2y'  -> 'H2H Wins 2y'
        'elo_diff'       -> 'ELO Diff'
    """
    # Strip trailing _a or _b (suffix pattern)
    base = re.sub(r"_[ab]$", "", feature_key)
    # Strip infix _a_ or _b_ (first occurrence only)
    base = re.sub(r"_[ab]_", "_", base, count=1)
    parts = base.split("_")
    display_parts = []
    for part in parts:
        if part in _DISPLAY_OVERRIDES:
            display_parts.append(_DISPLAY_OVERRIDES[part])
        else:
            display_parts.append(part.capitalize())
    return " ".join(display_parts)


def format_feature_value(value: Any, feature_key: str) -> str:
    """Format a feature value for display.

    - Rates/ratios (0-1 range, name contains 'rate' or 'ratio'): percentage
    - Integers: no decimals
    - Floats: 1-2 decimal places
    - None: dash
    """
    if value is None:
        return "\u2014"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (int, float)):
        is_rate = any(kw in feature_key for kw in ("rate", "ratio"))
        if is_rate and 0 <= value <= 1:
            return f"{value * 100:.0f}%"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        if isinstance(value, int):
            return str(value)
        return f"{value:.1f}"
    return str(value)


_SUFFIX_RE = re.compile(r"_[ab]$")
_INFIX_RE = re.compile(r"_([ab])_")

_SWAP_SUFFIX_RE = re.compile(r"_([ab])$")
_SWAP_INFIX_RE = re.compile(r"_([ab])_")


def swap_feature_sides(features: dict[str, Any]) -> dict[str, Any]:
    """Swap player A/B feature values so they match the swapped display order.

    Handles three patterns:
      - Suffix:  'elo_a' <-> 'elo_b'
      - Infix:   'h2h_a_wins' <-> 'h2h_b_wins'
      - Diff:    'elo_diff' -> negated (since diff = a - b)
      - Complement: 'h2h_a_dominance' -> 1 - value (since it's a / total)
    """
    swapped: dict[str, Any] = {}
    for key, value in features.items():
        # Suffix pattern: key ends with _a or _b
        m = _SWAP_SUFFIX_RE.search(key)
        if m:
            player = m.group(1)
            other = "b" if player == "a" else "a"
            partner_key = key[: m.start()] + f"_{other}"
            if partner_key in features:
                # Swap: put partner's value under this key
                swapped[key] = features[partner_key]
                continue

        # Infix pattern: key contains _a_ or _b_
        m = _SWAP_INFIX_RE.search(key)
        if m:
            player = m.group(1)
            other = "b" if player == "a" else "a"
            partner_key = key[: m.start()] + f"_{other}_" + key[m.end() :]
            if partner_key in features:
                swapped[key] = features[partner_key]
                continue

        # Diff features: negate the value
        if "diff" in key.lower() and isinstance(value, (int, float)):
            swapped[key] = -value
            continue

        # Dominance-style complement features
        if key == "h2h_a_dominance" and isinstance(value, (int, float)):
            swapped[key] = 1.0 - value
            continue

        # Non-player-specific features pass through unchanged
        swapped[key] = value

    return swapped


def _classify_player_key(key: str) -> tuple[str | None, str | None]:
    """Determine which player a feature belongs to and its base name.

    Handles two naming patterns:
      - Suffix: 'elo_a' / 'win_rate_8w_b'  -> player from trailing _a/_b
      - Infix:  'h2h_a_wins' / 'h2h_b_wins' -> player from _a_/_b_ in middle

    Returns (base_name, player) where player is 'a', 'b', or None.
    """
    # Suffix pattern: key ends with _a or _b
    if _SUFFIX_RE.search(key):
        return key[:-2], key[-1]
    # Infix pattern: key contains _a_ or _b_ (first occurrence)
    m = _INFIX_RE.search(key)
    if m:
        base = key[:m.start()] + "_" + key[m.end():]
        return base, m.group(1)
    return None, None


def pair_features(
    feature_keys: list[str],
    feature_values: dict[str, Any],
) -> list[dict]:
    """Pair player A/B features into comparison rows.

    Handles two naming patterns:
      - Suffix: 'elo_a' / 'elo_b' -> paired on base 'elo'
      - Infix:  'h2h_a_wins' / 'h2h_b_wins' -> paired on base 'h2h_wins'

    Returns a list of dicts, each with:
      - 'key': base feature name
      - 'display_name': human-readable name
      - 'type': 'paired' | 'single' | 'diff'
      - 'value_a': value for player A (paired only)
      - 'value_b': value for player B (paired only)
      - 'value': single value (single/diff only)
    """
    a_keys: dict[str, str] = {}  # base -> original key
    b_keys: dict[str, str] = {}
    other_keys: list[str] = []

    for key in feature_keys:
        base, player = _classify_player_key(key)
        if player == "a":
            a_keys[base] = key
        elif player == "b":
            b_keys[base] = key
        else:
            other_keys.append(key)

    rows = []
    seen_bases: set[str] = set()

    # Process paired features (both _a and _b exist for same base)
    for base in a_keys:
        if base in b_keys:
            seen_bases.add(base)
            rows.append({
                "key": base,
                "display_name": feature_display_name(a_keys[base]),
                "type": "paired",
                "value_a": feature_values.get(a_keys[base]),
                "value_b": feature_values.get(b_keys[base]),
            })

    # Process unpaired _a keys
    for base, key in a_keys.items():
        if base not in seen_bases:
            rows.append({
                "key": key,
                "display_name": feature_display_name(key),
                "type": "single",
                "value": feature_values.get(key),
            })

    # Process unpaired _b keys
    for base, key in b_keys.items():
        if base not in seen_bases:
            rows.append({
                "key": key,
                "display_name": feature_display_name(key),
                "type": "single",
                "value": feature_values.get(key),
            })

    # Process other keys (diff, standalone)
    for key in other_keys:
        row_type = "diff" if key.endswith("_diff") or "_diff_" in key else "single"
        rows.append({
            "key": key,
            "display_name": feature_display_name(key),
            "type": row_type,
            "value": feature_values.get(key),
        })

    return rows


def build_feature_groups(
    features: dict[str, Any],
    grouped_feature_names: dict[str, list[str]],
    neutral_groups: set[str],
) -> list[dict]:
    """Build structured feature group data for template rendering.

    Returns list of dicts:
      - 'key': group key
      - 'display_name': human-readable group name
      - 'neutral': bool (skip color coding)
      - 'rows': list from pair_features()
    """
    groups = []
    assigned_keys = set()

    for group_key, group_feature_names in grouped_feature_names.items():
        present_keys = [k for k in group_feature_names if k in features]
        if not present_keys:
            continue
        assigned_keys.update(present_keys)
        groups.append({
            "key": group_key,
            "display_name": group_display_name(group_key),
            "neutral": group_key in neutral_groups,
            "rows": pair_features(present_keys, features),
        })

    # Catch any features not in a known group
    unassigned = [k for k in features if k not in assigned_keys]
    if unassigned:
        groups.append({
            "key": "other",
            "display_name": "Other",
            "neutral": False,
            "rows": pair_features(unassigned, features),
        })

    return groups
