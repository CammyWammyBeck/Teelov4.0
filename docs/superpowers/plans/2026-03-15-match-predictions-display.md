# Match Predictions Display Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add prediction probabilities to match rows and create a match detail page with feature-by-feature comparison.

**Architecture:** Server-rendered Jinja2 templates with prediction data piped through the existing `serialize_match()` flow. New match detail page with collapsible feature group sections, dynamically driven by `MatchFeatures` JSONB data. Feature grouping uses `FeatureRegistry.grouped_features()` reverse mapping.

**Tech Stack:** Python/FastAPI, Jinja2, Tailwind CSS, vanilla JS, SQLAlchemy 2.0, PostgreSQL

**Spec:** `docs/superpowers/specs/2026-03-15-match-predictions-display-design.md`

---

## Chunk 1: Data Layer & Feature Registry

### Task 1: Add `grouped_features()` to FeatureRegistry

**Files:**
- Modify: `src/teelo/features/registry.py`

- [ ] **Step 1: Add `grouped_features()` method**

In `src/teelo/features/registry.py`, add to `FeatureRegistry` class after `compute_all()`:

```python
def grouped_features(self) -> dict[str, list[str]]:
    """Return {group_name: [feature_names]} for all registered groups."""
    result: dict[str, list[str]] = {}
    for group in self._groups:
        names = [n for n in group.feature_names() if n not in self._exclude]
        if names:
            result[group.name] = names
    return result
```

- [ ] **Step 2: Verify manually**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && python -c "from teelo.features import build_registry; r = build_registry('baseline_v2'); print(list(r.grouped_features().keys()))"`
Expected: List of group names like `['context', 'elo_core', 'elo_history', ...]`

- [ ] **Step 3: Commit**

```bash
git add src/teelo/features/registry.py
git commit -m "feat: add grouped_features() to FeatureRegistry"
```

### Task 2: Add `neutral_display` to FeatureGroup base class

**Files:**
- Modify: `src/teelo/features/registry.py`
- Modify: `src/teelo/features/groups/context.py`
- Modify: `src/teelo/features/groups/confidence.py`

- [ ] **Step 1: Add property to base class**

In `src/teelo/features/registry.py`, add to `FeatureGroup` class after `compute()`:

```python
@property
def neutral_display(self) -> bool:
    """If True, feature values are not color-coded as advantage/disadvantage."""
    return False
```

- [ ] **Step 2: Override in ContextFeatures**

In `src/teelo/features/groups/context.py`, add to the `ContextFeatures` class:

```python
@property
def neutral_display(self) -> bool:
    return True
```

- [ ] **Step 3: Override in ConfidenceFeatures**

In `src/teelo/features/groups/confidence.py`, add to the `ConfidenceFeatures` class:

```python
@property
def neutral_display(self) -> bool:
    return True
```

- [ ] **Step 4: Add `neutral_groups()` to FeatureRegistry**

In `src/teelo/features/registry.py`, add to `FeatureRegistry`:

```python
def neutral_groups(self) -> set[str]:
    """Return set of group names where values should not be color-coded."""
    return {g.name for g in self._groups if g.neutral_display}
```

- [ ] **Step 5: Commit**

```bash
git add src/teelo/features/registry.py src/teelo/features/groups/context.py src/teelo/features/groups/confidence.py
git commit -m "feat: add neutral_display property to FeatureGroup"
```

### Task 3: Create feature display utility

**Files:**
- Create: `src/teelo/web/services/feature_display.py`

- [ ] **Step 1: Create the utility module**

Create `src/teelo/web/services/feature_display.py`:

```python
"""Utilities for displaying ML features on the match detail page."""

from __future__ import annotations

import re
from typing import Any, Optional

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

    Strips _a/_b suffixes, replaces underscores, applies special cases.
    Examples:
        'elo_a' -> 'ELO'
        'win_rate_8w_a' -> 'Win Rate 8w'
        'h2h_a_dominance' -> 'H2H A Dominance'  (non-standard _a)
        'elo_diff' -> 'ELO Diff'
    """
    # Strip trailing _a or _b (player suffix)
    base = re.sub(r"_[ab]$", "", feature_key)
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
        return "—"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (int, float)):
        is_rate = any(kw in feature_key for kw in ("rate", "ratio"))
        if is_rate and 0 <= value <= 1:
            return f"{value * 100:.0f}%"
        if isinstance(value, int) or (isinstance(value, float) and value == int(value)):
            return str(int(value))
        return f"{value:.1f}"
    return str(value)


def pair_features(
    feature_keys: list[str],
    feature_values: dict[str, Any],
) -> list[dict]:
    """Pair _a/_b features into comparison rows.

    Returns a list of dicts, each with:
      - 'key': base feature name
      - 'display_name': human-readable name
      - 'type': 'paired' | 'single' | 'diff'
      - 'value_a': value for player A (paired only)
      - 'value_b': value for player B (paired only)
      - 'value': single value (single/diff only)
    """
    # Collect _a/_b pairs and standalone features
    a_keys = {}
    b_keys = {}
    other_keys = []

    for key in feature_keys:
        if key.endswith("_a"):
            base = key[:-2]
            a_keys[base] = key
        elif key.endswith("_b"):
            base = key[:-2]
            b_keys[base] = key
        else:
            other_keys.append(key)

    rows = []
    seen_bases = set()

    # Process paired features
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
        # Only include features that exist in the match's feature data
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
```

- [ ] **Step 2: Verify import**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && python -c "from teelo.web.services.feature_display import build_feature_groups, format_feature_value; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/services/feature_display.py
git commit -m "feat: add feature display utilities for match detail page"
```

### Task 4: Add prediction_a to serialize_match()

**Files:**
- Modify: `src/teelo/web/services/match_service.py`

- [ ] **Step 1: Add prediction_a to serialization**

In `src/teelo/web/services/match_service.py`, in the `serialize_match()` function:

After the `swap_display_sides` block (after line 96 `display_score = flip_score_for_display(display_score)`), add:

```python
    prediction_a_val = match.prediction_a
    if prediction_a_val is not None:
        prediction_a_val = float(prediction_a_val)
        if swap_display_sides:
            prediction_a_val = 1.0 - prediction_a_val
```

Then in the return dict (after `"year"` line), add:

```python
        "prediction_a": prediction_a_val,
        "match_url": f"/matches/{match.id}",
```

- [ ] **Step 2: Verify**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && python -c "from teelo.web.services.match_service import serialize_match; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/services/match_service.py
git commit -m "feat: add prediction_a and match_url to serialize_match()"
```

---

## Chunk 2: Match Row UI Updates

### Task 5: Update desktop table header and rows

**Files:**
- Modify: `src/teelo/web/templates/matches.html` (lines 136-142 for `<thead>`)
- Modify: `src/teelo/web/templates/partials/match_rows.html` (`render_table_rows` macro)

- [ ] **Step 1: Update `<thead>` in matches.html**

In `src/teelo/web/templates/matches.html`, replace lines 136-142:

```html
                    <tr class="bg-surface-alt/50 text-content-faint text-[11px] uppercase tracking-wider font-bold border-b border-line-subtle">
                        <th class="px-5 py-3">Tournament</th>
                        <th class="px-5 py-3 text-right">Player A</th>
                        <th class="px-5 py-3 text-center">Score</th>
                        <th class="px-5 py-3">Player B</th>
                        <th class="px-5 py-3 text-right">Date</th>
                    </tr>
```

Replace with:

```html
                    <tr class="bg-surface-alt/50 text-content-faint text-[11px] uppercase tracking-wider font-bold border-b border-line-subtle">
                        <th class="px-5 py-3">Tournament</th>
                        <th class="px-5 py-3 text-right">Player A</th>
                        <th class="px-5 py-3 text-center">Score</th>
                        <th class="px-5 py-3">Player B</th>
                        <th class="px-5 py-3 text-center">Pred</th>
                        <th class="px-5 py-3 text-right">Date</th>
                        <th class="w-8"></th>
                    </tr>
```

- [ ] **Step 2: Update `render_table_rows` macro in match_rows.html**

In `src/teelo/web/templates/partials/match_rows.html`, replace the entire `render_table_rows` macro (lines 43-106) with:

```html
{% macro render_table_rows(matches) %}
{% for m in matches %}
{% set is_winner_a = m.winner_id == m.player_a.id %}
{% set is_winner_b = m.winner_id == m.player_b.id %}
{% set player_a_url = m.player_a.player_url if m.player_a.player_url is defined and m.player_a.player_url else '/players/' ~ m.player_a.id %}
{% set player_b_url = m.player_b.player_url if m.player_b.player_url is defined and m.player_b.player_url else '/players/' ~ m.player_b.id %}
{% set score_display = 'W/O' if m.status == 'walkover' else (m.score or 'vs') %}
{% set pred_a = m.prediction_a if m.prediction_a is defined else none %}
{% set pred_b = (1 - pred_a) if pred_a is not none else none %}
{% set pred_a_pct = (pred_a * 100) | round(0) | int if pred_a is not none else none %}
{% set pred_b_pct = (100 - pred_a_pct) if pred_a_pct is not none else none %}
<tr class="hover:bg-surface-hover/50 transition-colors duration-75 group border-l-4 border-transparent hover:border-teelo-lime cursor-pointer" data-match-url="{{ m.match_url if m.match_url is defined else '/matches/' ~ m.id }}" role="link" tabindex="0">
    {% if m.player_result is defined and m.player_result %}
    <td class="pl-5 pr-2 py-3 w-8">
        <span class="inline-flex items-center justify-center w-6 h-6 rounded-full text-[11px] font-bold leading-none {% if m.player_result == 'W' %}bg-teelo-lime text-teelo-dark{% else %}bg-[var(--status-danger-bg)] text-status-danger{% endif %}">
            {{ m.player_result }}
        </span>
    </td>
    {% endif %}
    <td class="px-5 py-3">
        <div class="flex items-center gap-2">
            <span class="{{ circuit_bg(m.tour, m.tournament_level) }} text-content-inverse text-[10px] px-1.5 py-0.5 rounded font-bold tracking-tight flex-shrink-0">{{ circuit_label(m.tour, m.tournament_level) }}</span>
            <div class="min-w-0">
                {% if m.tournament_url %}
                <a href="{{ m.tournament_url }}" class="text-sm font-semibold text-teelo-dark truncate block hover:underline decoration-teelo-lime decoration-2" title="{{ m.tournament_name or 'Unknown' }}" onclick="event.stopPropagation()">{{ m.tournament_name or 'Unknown' }}</a>
                {% else %}
                <span class="text-sm font-semibold text-teelo-dark truncate block" title="{{ m.tournament_name or 'Unknown' }}">{{ m.tournament_name or 'Unknown' }}</span>
                {% endif %}
                <span class="text-xs text-content-faint inline-flex items-center gap-1.5 flex-wrap">
                    <span>{{ m.round or '' }}</span>
                    <span>·</span>
                    <span class="inline-flex items-center gap-1">
                        <span class="w-1.5 h-1.5 rounded-full {{ gender_dot_cls(m.gender, m.tour) }}"></span>
                        <span class="text-content-muted font-medium">{{ level_context(m.gender, m.tour, m.tournament_level) }}</span>
                    </span>
                    <span>·</span>
                    <span class="{{ surface_cls(m.surface) }}">{{ m.surface or '' }}</span>
                </span>
            </div>
        </div>
    </td>
    <td class="px-5 py-3 text-right">
        <div class="flex items-center justify-end gap-2">
            <a href="{{ player_a_url }}" class="text-sm hover:underline decoration-teelo-lime decoration-2 {% if is_winner_a %}text-teelo-dark font-bold{% else %}text-content-faint{% endif %}" onclick="event.stopPropagation()">{{ m.player_a.name }}</a>
            {{ elo_badge(m.player_a) }}
            {% if is_winner_a %}
            <i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>
            {% endif %}
        </div>
    </td>
    <td class="px-5 py-3 text-center">
        <span class="inline-block px-2.5 py-1 bg-surface-alt rounded-md text-xs font-mono whitespace-nowrap group-hover:bg-teelo-lime/10 transition-colors {% if score_display == 'vs' %}text-content-faint italic{% else %}text-teelo-dark font-semibold{% endif %}">{{ score_display }}</span>
    </td>
    <td class="px-5 py-3">
        <div class="flex items-center gap-2">
            {% if is_winner_b %}
            <i data-lucide="check" class="w-3.5 h-3.5 text-teelo-lime flex-shrink-0"></i>
            {% endif %}
            <a href="{{ player_b_url }}" class="text-sm hover:underline decoration-teelo-lime decoration-2 {% if is_winner_b %}text-teelo-dark font-bold{% else %}text-content-faint{% endif %}" onclick="event.stopPropagation()">{{ m.player_b.name }}</a>
            {{ elo_badge(m.player_b) }}
        </div>
    </td>
    <td class="px-3 py-3 text-center">
        {% if pred_a_pct is not none %}
        <div class="flex flex-col items-center gap-0.5 text-[11px] leading-tight">
            <span class="{{ 'text-teelo-dark font-semibold' if pred_a_pct >= pred_b_pct else 'text-content-faint' }}">{{ pred_a_pct }}%</span>
            <span class="{{ 'text-teelo-dark font-semibold' if pred_b_pct > pred_a_pct else 'text-content-faint' }}">{{ pred_b_pct }}%</span>
        </div>
        {% else %}
        <span class="text-content-faintest text-[11px]">—</span>
        {% endif %}
    </td>
    <td class="px-5 py-3 text-right">
        <span class="text-xs text-content-faint whitespace-nowrap">{{ m.match_date_display or '' }}</span>
    </td>
    <td class="pr-3 py-3 w-8">
        <a href="{{ m.match_url if m.match_url is defined else '/matches/' ~ m.id }}" class="text-content-faintest group-hover:text-teelo-lime transition-colors" onclick="event.stopPropagation()" aria-label="View match details">
            <i data-lucide="chevron-right" class="w-4 h-4"></i>
        </a>
    </td>
</tr>
{% endfor %}
{% endmacro %}
```

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/templates/matches.html src/teelo/web/templates/partials/match_rows.html
git commit -m "feat: add prediction column and click-to-detail to desktop match rows"
```

### Task 6: Update mobile cards

**Files:**
- Modify: `src/teelo/web/templates/partials/match_rows.html` (`render_cards` macro)

- [ ] **Step 1: Replace `render_cards` macro**

In `src/teelo/web/templates/partials/match_rows.html`, replace the entire `render_cards` macro (lines 108-162 approx, after the `render_table_rows` endmacro) with:

```html
{% macro render_cards(matches) %}
{% for m in matches %}
{% set is_winner_a = m.winner_id == m.player_a.id %}
{% set is_winner_b = m.winner_id == m.player_b.id %}
{% set has_winner = is_winner_a or is_winner_b %}
{% set player_a_url = m.player_a.player_url if m.player_a.player_url is defined and m.player_a.player_url else '/players/' ~ m.player_a.id %}
{% set player_b_url = m.player_b.player_url if m.player_b.player_url is defined and m.player_b.player_url else '/players/' ~ m.player_b.id %}
{% set score_display = 'W/O' if m.status == 'walkover' else (m.score or 'vs') %}
{% set pred_a = m.prediction_a if m.prediction_a is defined else none %}
{% set pred_b = (1 - pred_a) if pred_a is not none else none %}
{% set pred_a_pct = (pred_a * 100) | round(0) | int if pred_a is not none else none %}
{% set pred_b_pct = (100 - pred_a_pct) if pred_a_pct is not none else none %}
<div class="px-4 py-3 border-b border-line-subtle last:border-b-0 cursor-pointer hover:bg-surface-hover/50 transition-colors" data-match-url="{{ m.match_url if m.match_url is defined else '/matches/' ~ m.id }}" role="link" tabindex="0">
    {# Tournament header — badge + name get full width #}
    <div class="flex items-center gap-2 mb-0.5">
        <span class="{{ circuit_bg(m.tour, m.tournament_level) }} text-content-inverse text-[10px] px-1.5 py-0.5 rounded font-bold tracking-tight flex-shrink-0">{{ circuit_label(m.tour, m.tournament_level) }}</span>
        {% if m.tournament_url %}
        <a href="{{ m.tournament_url }}" class="text-[13px] font-semibold text-teelo-dark truncate hover:underline decoration-teelo-lime decoration-2" onclick="event.stopPropagation()">{{ m.tournament_name or 'Unknown' }}</a>
        {% else %}
        <span class="text-[13px] font-semibold text-teelo-dark truncate">{{ m.tournament_name or 'Unknown' }}</span>
        {% endif %}
        <a href="{{ m.match_url if m.match_url is defined else '/matches/' ~ m.id }}" class="ml-auto text-content-faintest flex-shrink-0" onclick="event.stopPropagation()" aria-label="View match details">
            <i data-lucide="chevron-right" class="w-4 h-4"></i>
        </a>
    </div>

    {# Meta line — compact: round · gender-dot surface · date #}
    <div class="flex items-center gap-1.5 text-[11px] text-content-faint mb-2">
        {% if m.player_result is defined and m.player_result %}
        <span class="inline-flex items-center justify-center w-5 h-5 rounded-full text-[10px] font-bold leading-none flex-shrink-0 {% if m.player_result == 'W' %}bg-teelo-lime text-teelo-dark{% else %}bg-[var(--status-danger-bg)] text-status-danger{% endif %}">{{ m.player_result }}</span>
        <span class="text-content-faintest">·</span>
        {% endif %}
        <span class="font-medium">{{ m.round or '' }}</span>
        <span class="text-content-faintest">·</span>
        <span class="w-1.5 h-1.5 rounded-full {{ gender_dot_cls(m.gender, m.tour) }} flex-shrink-0"></span>
        <span class="{{ surface_cls(m.surface) }} font-medium">{{ m.surface or '' }}</span>
        <span class="text-content-faintest">·</span>
        <span>{{ m.match_date_display or '' }}</span>
    </div>

    {# Prediction bar (if available) #}
    {% if pred_a_pct is not none %}
    <div class="flex items-center gap-2 text-[11px] mb-2">
        <span class="{{ 'font-semibold text-teelo-dark' if pred_a_pct >= pred_b_pct else 'text-content-faint' }}">{{ pred_a_pct }}%</span>
        <div class="flex-1 h-1 rounded-full bg-surface-muted overflow-hidden">
            <div class="h-full bg-teelo-lime rounded-full" style="width: {{ pred_a_pct }}%"></div>
        </div>
        <span class="{{ 'font-semibold text-teelo-dark' if pred_b_pct > pred_a_pct else 'text-content-faint' }}">{{ pred_b_pct }}%</span>
    </div>
    {% endif %}

    {# Scoreboard — players left with winner accent bar, score right-aligned #}
    <div class="flex items-center">
        <div class="flex-1 min-w-0 space-y-1">
            {# Player A #}
            <div class="flex items-center gap-1.5">
                <div class="w-0.5 h-4 rounded-full {{ 'bg-teelo-lime' if is_winner_a else 'bg-transparent' }} flex-shrink-0"></div>
                <a href="{{ player_a_url }}" class="text-[13px] truncate hover:underline decoration-teelo-lime decoration-2 {{ 'font-bold text-teelo-dark' if is_winner_a else ('text-content-faint' if has_winner else 'text-teelo-dark font-medium') }}" onclick="event.stopPropagation()">{{ m.player_a.name }}</a>
                {{ elo_compact(m.player_a) }}
            </div>
            {# Player B #}
            <div class="flex items-center gap-1.5">
                <div class="w-0.5 h-4 rounded-full {{ 'bg-teelo-lime' if is_winner_b else 'bg-transparent' }} flex-shrink-0"></div>
                <a href="{{ player_b_url }}" class="text-[13px] truncate hover:underline decoration-teelo-lime decoration-2 {{ 'font-bold text-teelo-dark' if is_winner_b else ('text-content-faint' if has_winner else 'text-teelo-dark font-medium') }}" onclick="event.stopPropagation()">{{ m.player_b.name }}</a>
                {{ elo_compact(m.player_b) }}
            </div>
        </div>
        {# Score — vertically centered to the right of the player block #}
        <span class="text-xs font-mono whitespace-nowrap flex-shrink-0 ml-3 {{ 'text-content-faintest italic font-sans' if score_display == 'vs' else ('text-content-faint font-sans font-medium' if score_display == 'W/O' else 'text-teelo-dark font-semibold') }}">{{ score_display }}</span>
    </div>
</div>
{% endfor %}
{% endmacro %}
```

- [ ] **Step 2: Commit**

```bash
git add src/teelo/web/templates/partials/match_rows.html
git commit -m "feat: add prediction display and click-to-detail to mobile match cards"
```

### Task 7: Add row click handler in JS

**Files:**
- Modify: `src/teelo/web/static/js/controllers/matches_controller.js`

- [ ] **Step 1: Add click handler via event delegation**

In `src/teelo/web/static/js/controllers/matches_controller.js`, find the end of the `initMatchesPage()` function (before the closing `}`). Add this event delegation handler:

```javascript
    // Match row click-to-detail navigation
    function handleMatchRowClick(e) {
        // Don't navigate if user clicked a link inside the row
        if (e.target.closest('a')) return;
        const row = e.target.closest('[data-match-url]');
        if (row) {
            window.location.href = row.dataset.matchUrl;
        }
    }
    function handleMatchRowKeydown(e) {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            const row = e.target.closest('[data-match-url]');
            if (row) {
                window.location.href = row.dataset.matchUrl;
            }
        }
    }
    const matchesWrapper = document.getElementById('matches-results-wrapper');
    if (matchesWrapper) {
        matchesWrapper.addEventListener('click', handleMatchRowClick);
        matchesWrapper.addEventListener('keydown', handleMatchRowKeydown);
    }
```

- [ ] **Step 2: Commit**

```bash
git add src/teelo/web/static/js/controllers/matches_controller.js
git commit -m "feat: add click-to-detail event delegation for match rows"
```

### Task 8: Update fallback JS renderers

**Files:**
- Modify: `src/teelo/web/static/js/renderers/matches.js`

- [ ] **Step 1: Update `buildFallbackTableRows()`**

In `src/teelo/web/static/js/renderers/matches.js`, update `buildFallbackTableRows()` to include prediction column and chevron. Find the function and replace it:

```javascript
export function buildFallbackTableRows(matches) {
    return matches.map(m => {
        const predA = m.prediction_a != null ? Math.round(m.prediction_a * 100) : null;
        const predB = predA != null ? 100 - predA : null;
        const predHtml = predA != null
            ? `<div class="flex flex-col items-center gap-0.5 text-[11px] leading-tight">
                 <span class="${predA >= predB ? 'text-teelo-dark font-semibold' : 'text-content-faint'}">${predA}%</span>
                 <span class="${predB > predA ? 'text-teelo-dark font-semibold' : 'text-content-faint'}">${predB}%</span>
               </div>`
            : '<span class="text-content-faintest text-[11px]">—</span>';
        const matchUrl = m.match_url || `/matches/${m.id}`;
        return `<tr class="hover:bg-surface-hover/50 transition-colors group border-l-4 border-transparent hover:border-teelo-lime cursor-pointer" data-match-url="${escapeHtml(matchUrl)}" role="link" tabindex="0">
            <td class="px-5 py-3"><span class="text-sm font-semibold">${escapeHtml(m.tournament_name || '')}</span></td>
            <td class="px-5 py-3 text-right"><span class="text-sm">${escapeHtml(m.player_a?.name || '')}</span></td>
            <td class="px-5 py-3 text-center"><span class="text-xs font-mono">${escapeHtml(m.score || 'vs')}</span></td>
            <td class="px-5 py-3"><span class="text-sm">${escapeHtml(m.player_b?.name || '')}</span></td>
            <td class="px-3 py-3 text-center">${predHtml}</td>
            <td class="px-5 py-3 text-right"><span class="text-xs text-content-faint">${escapeHtml(m.match_date_display || '')}</span></td>
            <td class="pr-3 py-3 w-8"><a href="${escapeHtml(matchUrl)}" class="text-content-faintest group-hover:text-teelo-lime" onclick="event.stopPropagation()"><i data-lucide="chevron-right" class="w-4 h-4"></i></a></td>
        </tr>`;
    }).join('');
}
```

- [ ] **Step 2: Update `buildFallbackCards()` similarly**

```javascript
export function buildFallbackCards(matches) {
    return matches.map(m => {
        const predA = m.prediction_a != null ? Math.round(m.prediction_a * 100) : null;
        const predB = predA != null ? 100 - predA : null;
        const predHtml = predA != null
            ? `<div class="flex items-center gap-2 text-[11px] mb-2">
                 <span class="${predA >= predB ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predA}%</span>
                 <div class="flex-1 h-1 rounded-full bg-surface-muted overflow-hidden"><div class="h-full bg-teelo-lime rounded-full" style="width:${predA}%"></div></div>
                 <span class="${predB > predA ? 'font-semibold text-teelo-dark' : 'text-content-faint'}">${predB}%</span>
               </div>`
            : '';
        const matchUrl = m.match_url || `/matches/${m.id}`;
        return `<div class="px-4 py-3 border-b border-line-subtle last:border-b-0 cursor-pointer hover:bg-surface-hover/50" data-match-url="${escapeHtml(matchUrl)}" role="link" tabindex="0">
            <div class="flex items-center gap-2 mb-0.5">
                <span class="text-[13px] font-semibold text-teelo-dark truncate">${escapeHtml(m.tournament_name || '')}</span>
                <a href="${escapeHtml(matchUrl)}" class="ml-auto text-content-faintest" onclick="event.stopPropagation()"><i data-lucide="chevron-right" class="w-4 h-4"></i></a>
            </div>
            ${predHtml}
            <div class="flex items-center">
                <div class="flex-1 min-w-0 space-y-1">
                    <div class="text-[13px]">${escapeHtml(m.player_a?.name || '')}</div>
                    <div class="text-[13px]">${escapeHtml(m.player_b?.name || '')}</div>
                </div>
                <span class="text-xs font-mono ml-3">${escapeHtml(m.score || 'vs')}</span>
            </div>
        </div>`;
    }).join('');
}
```

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/static/js/renderers/matches.js
git commit -m "feat: update fallback JS renderers with prediction and chevron"
```

### Task 9: Rebuild Tailwind CSS

- [ ] **Step 1: Rebuild**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify`

- [ ] **Step 2: Commit if changed**

```bash
git add src/teelo/web/static/css/styles.css
git commit -m "chore: rebuild Tailwind CSS"
```

---

## Chunk 3: Match Detail Page

### Task 10: Add match detail route

**Files:**
- Modify: `src/teelo/web/routers/matches.py`

- [ ] **Step 1: Add imports and route**

At the top of `src/teelo/web/routers/matches.py`, add these imports:

```python
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session, contains_eager, joinedload

from teelo.db.models import Match, MatchFeatures, FeatureSet, Player, PlayerAlias, Tournament, TournamentEdition
from teelo.features import build_registry, default_preset_for_feature_set
from teelo.web.services.feature_display import build_feature_groups, format_feature_value
from teelo.web.services.match_service import resolve_date_preset, serialize_match, slugify_name
```

(This replaces the existing imports — merge with what's already there, adding the new ones: `Request`, `HTMLResponse`, `MatchFeatures`, `FeatureSet`, `build_registry`, `default_preset_for_feature_set`, `build_feature_groups`, `format_feature_value`, `slugify_name`.)

Then add the route at the end of the file:

```python
@router.get('/matches/{match_id}')
async def match_detail(match_id: int, request: Request, db: Session = Depends(get_db)):
    match = (
        db.query(Match)
        .options(
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament),
        )
        .filter(Match.id == match_id)
        .first()
    )
    if not match:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

    match_data = serialize_match(match)

    # Load feature data (most recent feature set)
    match_features = (
        db.query(MatchFeatures)
        .options(joinedload(MatchFeatures.feature_set))
        .filter(MatchFeatures.match_id == match_id)
        .order_by(MatchFeatures.computed_at.desc())
        .first()
    )

    feature_groups = []
    feature_set_info = None
    if match_features and match_features.features:
        feature_set = match_features.feature_set
        feature_set_info = {
            "name": feature_set.name,
            "version": feature_set.version,
            "description": feature_set.description,
        }
        preset = default_preset_for_feature_set(feature_set.name)
        registry = build_registry(preset)
        grouped = registry.grouped_features()
        neutral = registry.neutral_groups()
        feature_groups = build_feature_groups(
            match_features.features, grouped, neutral,
        )

    return templates.TemplateResponse("match_detail.html", {
        "request": request,
        "match": match_data,
        "feature_groups": feature_groups,
        "feature_set": feature_set_info,
        "format_value": format_feature_value,
    })
```

- [ ] **Step 2: Verify import**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && python -c "from teelo.web.routers.matches import router; print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add src/teelo/web/routers/matches.py
git commit -m "feat: add match detail route with feature data loading"
```

### Task 11: Create match detail template

**Files:**
- Create: `src/teelo/web/templates/match_detail.html`

- [ ] **Step 1: Create the template**

Create `src/teelo/web/templates/match_detail.html`:

```html
{% extends "base.html" %}
{% from "partials/match_rows.html" import circuit_bg, circuit_label, surface_cls %}

{% block title %}{{ match.player_a.name }} vs {{ match.player_b.name }} - Teelo{% endblock %}

{% block content %}
<div class="max-w-4xl mx-auto px-4 py-8">

    {# ── Prediction Hero ── #}
    <div class="bg-white rounded-2xl shadow-soft border border-gray-100 p-6 md:p-8 mb-6">
        {% if match.prediction_a is not none %}
        {% set pred_a_pct = (match.prediction_a * 100) | round(0) | int %}
        {% set pred_b_pct = 100 - pred_a_pct %}
        <div class="flex items-center justify-between mb-4">
            <a href="{{ match.player_a.player_url }}" class="text-lg md:text-xl font-bold text-teelo-dark hover:underline decoration-teelo-lime decoration-2">{{ match.player_a.name }}</a>
            <a href="{{ match.player_b.player_url }}" class="text-lg md:text-xl font-bold text-teelo-dark hover:underline decoration-teelo-lime decoration-2 text-right">{{ match.player_b.name }}</a>
        </div>
        <div class="flex items-center gap-4 mb-3">
            <span class="text-3xl md:text-4xl font-black {{ 'text-teelo-dark' if pred_a_pct >= pred_b_pct else 'text-content-faint' }}">{{ pred_a_pct }}%</span>
            <div class="flex-1 h-3 rounded-full bg-surface-muted overflow-hidden">
                <div class="h-full bg-teelo-lime rounded-full transition-all" style="width: {{ pred_a_pct }}%"></div>
            </div>
            <span class="text-3xl md:text-4xl font-black {{ 'text-teelo-dark' if pred_b_pct > pred_a_pct else 'text-content-faint' }}">{{ pred_b_pct }}%</span>
        </div>
        {% if feature_set %}
        <p class="text-xs text-content-faintest text-center">Model: {{ feature_set.name }} v{{ feature_set.version }}</p>
        {% endif %}
        {% else %}
        <div class="text-center py-4">
            <div class="flex items-center justify-between mb-4">
                <a href="{{ match.player_a.player_url }}" class="text-lg md:text-xl font-bold text-teelo-dark hover:underline decoration-teelo-lime decoration-2">{{ match.player_a.name }}</a>
                <span class="text-content-faint font-medium">vs</span>
                <a href="{{ match.player_b.player_url }}" class="text-lg md:text-xl font-bold text-teelo-dark hover:underline decoration-teelo-lime decoration-2 text-right">{{ match.player_b.name }}</a>
            </div>
            <p class="text-sm text-content-faint">No prediction available</p>
        </div>
        {% endif %}
    </div>

    {# ── Match Info Header ── #}
    <div class="bg-white rounded-2xl shadow-soft border border-gray-100 p-5 md:p-6 mb-6">
        <div class="flex items-center gap-2 mb-2">
            <span class="{{ circuit_bg(match.tour, match.tournament_level) }} text-content-inverse text-xs px-2 py-0.5 rounded font-bold tracking-tight">{{ circuit_label(match.tour, match.tournament_level) }}</span>
            {% if match.tournament_url %}
            <a href="{{ match.tournament_url }}" class="text-base font-semibold text-teelo-dark hover:underline decoration-teelo-lime decoration-2">{{ match.tournament_name or 'Unknown' }}</a>
            {% else %}
            <span class="text-base font-semibold text-teelo-dark">{{ match.tournament_name or 'Unknown' }}</span>
            {% endif %}
        </div>
        <div class="flex flex-wrap items-center gap-2 text-sm text-content-muted">
            <span class="font-medium">{{ match.round or '' }}</span>
            <span class="text-content-faintest">·</span>
            <span class="{{ surface_cls(match.surface) }} font-medium">{{ match.surface or '' }}</span>
            <span class="text-content-faintest">·</span>
            <span>{{ match.match_date_display or '' }}</span>
            {% if match.tournament_level %}
            <span class="text-content-faintest">·</span>
            <span>{{ match.tournament_level }}</span>
            {% endif %}
        </div>
        {% if match.score %}
        <div class="mt-4 flex items-center gap-3">
            <span class="text-lg font-mono font-semibold">{{ match.score }}</span>
            {% if match.status and match.status not in ['completed', 'scheduled'] %}
            <span class="text-xs text-content-faint bg-surface-muted px-2 py-0.5 rounded">{{ match.status | capitalize }}</span>
            {% endif %}
        </div>
        {% endif %}
    </div>

    {# ── Feature Comparison ── #}
    {% if feature_groups %}
    <div class="space-y-4">
        <h2 class="text-lg font-bold text-teelo-dark">Prediction Breakdown</h2>
        {% for group in feature_groups %}
        <div class="bg-white rounded-2xl shadow-soft border border-gray-100 overflow-hidden">
            <button class="feature-group-toggle w-full flex items-center justify-between px-5 py-4 text-left hover:bg-surface-hover/50 transition-colors" aria-expanded="{{ 'true' if loop.index <= 3 else 'false' }}" data-target="feature-group-{{ loop.index }}">
                <span class="text-sm font-bold text-teelo-dark">{{ group.display_name }}</span>
                <i data-lucide="chevron-down" class="w-4 h-4 text-content-faint transition-transform {{ '' if loop.index <= 3 else 'rotate-[-90deg]' }}"></i>
            </button>
            <div id="feature-group-{{ loop.index }}" class="feature-group-content {{ '' if loop.index <= 3 else 'hidden' }}">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="bg-surface-alt/30 text-[11px] uppercase tracking-wider text-content-faint">
                            <th class="px-5 py-2 text-left font-bold">Feature</th>
                            <th class="px-5 py-2 text-right font-bold">{{ match.player_a.name.split(' ')[-1] }}</th>
                            <th class="px-5 py-2 text-right font-bold">{{ match.player_b.name.split(' ')[-1] }}</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-line-subtle/50">
                        {% for row in group.rows %}
                        {% if row.type == 'paired' %}
                        {% set va = row.value_a %}
                        {% set vb = row.value_b %}
                        {% set a_better = va is not none and vb is not none and va > vb %}
                        {% set b_better = va is not none and vb is not none and vb > va %}
                        <tr class="hover:bg-surface-hover/30">
                            <td class="px-5 py-2 text-content-muted">{{ row.display_name }}</td>
                            <td class="px-5 py-2 text-right font-medium {% if not group.neutral %}{{ 'text-status-success' if a_better else ('text-status-danger' if b_better else '') }}{% endif %}">{{ format_value(va, row.key + '_a') }}</td>
                            <td class="px-5 py-2 text-right font-medium {% if not group.neutral %}{{ 'text-status-success' if b_better else ('text-status-danger' if a_better else '') }}{% endif %}">{{ format_value(vb, row.key + '_b') }}</td>
                        </tr>
                        {% else %}
                        <tr class="hover:bg-surface-hover/30">
                            <td class="px-5 py-2 text-content-muted">{{ row.display_name }}</td>
                            <td class="px-5 py-2 text-right font-medium text-content-muted" colspan="2">{{ format_value(row.value, row.key) }}</td>
                        </tr>
                        {% endif %}
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        {% endfor %}
    </div>
    {% else %}
    <div class="bg-white rounded-2xl shadow-soft border border-gray-100 p-8 text-center">
        <i data-lucide="bar-chart-3" class="w-10 h-10 text-content-faintest mx-auto mb-3"></i>
        <p class="text-sm text-content-faint">No feature data available for this match</p>
    </div>
    {% endif %}

</div>
{% endblock %}

{% block scripts %}
<script src="{{ url_for('static', path='/js/match_detail.js') }}" type="module"></script>
{% endblock %}
```

- [ ] **Step 2: Commit**

```bash
git add src/teelo/web/templates/match_detail.html
git commit -m "feat: create match detail page template"
```

### Task 12: Create match detail JS

**Files:**
- Create: `src/teelo/web/static/js/match_detail.js`

- [ ] **Step 1: Create the JS module**

Create `src/teelo/web/static/js/match_detail.js`:

```javascript
// Match detail page — collapsible feature group sections

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.feature-group-toggle').forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.dataset.target;
            const content = document.getElementById(targetId);
            if (!content) return;

            const isExpanded = btn.getAttribute('aria-expanded') === 'true';
            btn.setAttribute('aria-expanded', !isExpanded);
            content.classList.toggle('hidden');

            // Rotate chevron icon
            const icon = btn.querySelector('[data-lucide]');
            if (icon) {
                icon.classList.toggle('rotate-[-90deg]', isExpanded);
            }
        });
    });

    // Re-init Lucide icons for any dynamic content
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
});
```

- [ ] **Step 2: Commit**

```bash
git add src/teelo/web/static/js/match_detail.js
git commit -m "feat: add match detail page JS for collapsible sections"
```

### Task 13: Rebuild Tailwind CSS and verify

- [ ] **Step 1: Rebuild Tailwind**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify`

- [ ] **Step 2: Start the dev server and test**

Run: `cd /home/cammybeck/Documents/programming/Teelov4.0 && source venv/bin/activate && uvicorn teelo.api.main:app --reload`

Verify:
1. `/` shows match rows with prediction column and chevron
2. Clicking a match row navigates to `/matches/{id}`
3. Match detail page shows prediction hero, match info, and feature groups (if data exists)
4. Player/tournament links work without navigating to match detail
5. Mobile cards show prediction bar and chevron

- [ ] **Step 3: Commit CSS if changed**

```bash
git add src/teelo/web/static/css/styles.css
git commit -m "chore: rebuild Tailwind CSS for match detail page"
```
