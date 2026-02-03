# WTA Scraper Implementation Plan

## Overview

This document outlines the implementation plan for completing the WTA scraper with draw and schedule scraping, following the same pipeline pattern as ATP.

---

## Match Lifecycle Pipeline (ATP Reference)

The ATP scraper follows a three-stage pipeline where match data is progressively enriched:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  1. DRAW        │───▶│  2. SCHEDULE     │───▶│  3. RESULTS     │
│  (Days before)  │    │  (Day of match)  │    │  (After match)  │
│  status=upcoming│    │ status=scheduled │    │ status=completed│
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Stage 1: Draw Scraping

**When:** Days/weeks before tournament starts (once draw is released)
**Source:** `/draws` page
**Creates:** Match records with `status='upcoming'`

| Field | Populated | Notes |
|-------|-----------|-------|
| `player_a_id` | ✅ | Resolved from name/external ID |
| `player_b_id` | ✅ | Resolved from name/external ID |
| `player_a_seed` | ✅ | From draw entry |
| `player_b_seed` | ✅ | From draw entry |
| `round` | ✅ | R128, R64, R32, R16, QF, SF, F |
| `draw_position` | ✅ | 1-indexed position in round |
| `tournament_edition_id` | ✅ | Links to tournament |
| `external_id` | ✅ | For deduplication across stages |
| `status` | ✅ | `'upcoming'` |
| `scheduled_date` | ❌ | Not known yet |
| `court` | ❌ | Not known yet |
| `score` | ❌ | Match not played |
| `winner_id` | ❌ | Match not played |

**Ingestion Service:** `ingest_draw()` from `teelo/services/draw_ingestion.py`

### Stage 2: Schedule Scraping

**When:** Day of match (or day before, when order of play is released)
**Source:** `/order-of-play` page (ATP: `/daily-schedule`)
**Updates:** Existing match records from `status='upcoming'` to `status='scheduled'`

| Field | Updated | Notes |
|-------|---------|-------|
| `scheduled_date` | ✅ | Date match is scheduled |
| `scheduled_datetime` | ✅ | Full datetime if time available |
| `court` | ✅ | Court assignment |
| `status` | ✅ | `'upcoming'` → `'scheduled'` |

**Matching:** Uses `external_id` to find existing match from draw stage

**Ingestion Service:** `ingest_schedule()` from `teelo/services/schedule_ingestion.py`

### Stage 3: Results Scraping

**When:** After match completes
**Source:** `/scores` page (ATP: `/results`)
**Updates:** Existing match records from `status='scheduled'` (or `'upcoming'`) to `status='completed'`

| Field | Updated | Notes |
|-------|---------|-------|
| `score` | ✅ | Raw score string |
| `score_structured` | ✅ | Parsed JSON |
| `winner_id` | ✅ | FK to winning player |
| `match_date` | ✅ | Actual match date |
| `duration_minutes` | ✅ | If available |
| `status` | ✅ | `'completed'`, `'retired'`, `'walkover'` |

**Matching:** Uses `external_id` to find existing match from draw/schedule stages

**Ingestion Service:** `ingest_results()` from `teelo/services/results_ingestion.py`

### External ID Format (Critical for Pipeline)

All three stages must generate **identical external IDs** for the same match:

```python
external_id = f"{year}_{tournament_id}_{round_code}_{sorted_player_ids[0]}_{sorted_player_ids[1]}"
# Example: "2026_2088_R16_319001_329918"
```

- `tournament_id`: Slug (e.g., `abu-dhabi`) or number depending on tour. Check what the result scraping that is already implemented uses.
- `round_code`: Normalized (`R128`, `R64`, `R32`, `R16`, `QF`, `SF`, `F`)
- Player IDs: **Sorted alphabetically** for consistency regardless of player order

---

## Current WTA Scraper Status

### ✅ Already Implemented (`src/teelo/scrape/wta.py`)

| Method | Status | Notes |
|--------|--------|-------|
| `get_tournament_list()` | ✅ Complete | Main tour + WTA 125 |
| `scrape_tournament_results()` | ✅ Complete | Day navigation, scores page |
| `_parse_scores_day()` | ✅ Complete | Singles match parsing |
| `_scrape_draws_page()` | ⚠️ Partial | Fallback for results only, not draw entries |
| `scrape_fixtures()` | ❌ Stub | Placeholder, not implemented |

### ❌ Missing (Need to Implement)

| Method | Purpose |
|--------|---------|
| `scrape_tournament_draw()` | Full draw extraction with `ScrapedDrawEntry` objects |
| `scrape_fixtures()` | Order of play with scheduled times |

---

## URL Patterns

| Page Type | URL Pattern | Example |
|-----------|-------------|---------|
| Tournament List | `/tournaments?status=all&year={year}` | `?year=2026` |
| WTA 125 List | `/tournaments/wta-125?year={year}&status=all` | `?year=2026` |
| Draws | `/tournaments/{number}/{slug}/{year}/draws` | `/2088/abu-dhabi/2026/draws` |
| Schedule | `/tournaments/{number}/{slug}/{year}/order-of-play` | `/2088/abu-dhabi/2026/order-of-play` |
| Results | `/tournaments/{number}/{slug}/{year}/scores` | `/2088/abu-dhabi/2026/scores` |

**Note:** WTA uses numeric tournament IDs (e.g., `2088`) in URLs, stored as `tournament_number`.

---

## Implementation Tasks

### Task 1: Implement `scrape_tournament_draw()` ← NEW

**Returns:** `list[ScrapedDrawEntry]` (not `ScrapedMatch`)

This method should return draw entries that can be processed by `ingest_draw()`.

```python
async def scrape_tournament_draw(
    self,
    tournament_id: str,      # slug like "abu-dhabi"
    year: int,
    tournament_number: str,  # WTA number like "2088"
    draw_type: str = "singles",  # "singles", "doubles", "qualifying"
) -> list[ScrapedDrawEntry]:
```

**Key HTML Elements:**

| Data | Selector | Example |
|------|----------|---------|
| Round container | `div[data-event-type='LS']` | Singles draw |
| Round title | `h2.tournament-draw__round-title` | "Semi-Finals" |
| Match table | `table.match-table` | - |
| Player link | `a.match-table__player--link` | `/players/319001/belinda-bencic` |
| Seed | `span.match-table__player-seed` | "(1)" |
| Winner class | `match-table--winner-a` or `match-table--winner-b` | On table |
| Score cells | `td.match-table__score-cell` | Set scores |

**Draw Position Calculation:**
```python
# Within each round, position = match index + 1
# R128: positions 1-64 (64 matches)
# R64: positions 1-32
# R32: positions 1-16
# R16: positions 1-8
# QF: positions 1-4
# SF: positions 1-2
# F: position 1
```

**ScrapedDrawEntry Fields to Populate:**
```python
ScrapedDrawEntry(
    round=round_code,              # "R64", "QF", etc.
    draw_position=position,         # 1-indexed within round
    player_a_name=name_a,
    player_a_external_id=wta_id_a,  # Numeric like "319001"
    player_a_seed=seed_a,
    player_b_name=name_b,
    player_b_external_id=wta_id_b,
    player_b_seed=seed_b,
    score_raw=score,               # If match completed
    winner_name=winner,            # If match completed
    is_bye=is_bye,
    source="wta",
    tournament_name=name,
    tournament_id=tournament_id,
    tournament_year=year,
    tournament_level=level,
    tournament_surface=surface,
)
```

---

### Task 2: Implement `scrape_fixtures()` ← NEW

**Returns:** `AsyncGenerator[ScrapedFixture, None]`

```python
async def scrape_fixtures(
    self,
    tournament_id: str,
    year: int,
    tournament_number: str,
) -> AsyncGenerator[ScrapedFixture, None]:
```

**Key HTML Elements:**

| Data | Selector | Example |
|------|----------|---------|
| Day buttons | `button.day-navigation__button` | Click to load day |
| Court container | `div.tournament-oop__court` | Groups by court |
| Court name | `h3.court-header__name` | "Stadium Court" |
| Match container | `div.tennis-match` | Individual match |
| Round | `div.tennis-match__round` | "Round of 16" |
| Player link | `a.match-table__player--link` | Player info |
| Scheduled time | `div.tennis-match__status-time` | "Not before: 13:00" |
| Status label | `div.tennis-match__upcoming-label` | "Upcoming" |

**Time Parsing:**
```python
# Various formats to handle:
"Not before: 13:00" → parse time, scheduled_date from day button
"Follows previous match" → no specific time
"Starts at 10:30 PM" → parse time
```

**ScrapedFixture Fields:**
```python
ScrapedFixture(
    tournament_name=name,
    tournament_id=tournament_id,
    tournament_year=year,
    tournament_level=level,
    tournament_surface=surface,
    round=round_code,
    scheduled_date=date_str,       # "2026-02-03"
    scheduled_time=time_str,       # "13:00"
    court=court_name,
    player_a_name=name_a,
    player_a_external_id=wta_id_a,
    player_a_seed=seed_a,
    player_b_name=name_b,
    player_b_external_id=wta_id_b,
    player_b_seed=seed_b,
    source="wta",
)
```

---

### Task 3: Update External ID Format

Ensure all three methods generate compatible external IDs:

**Current (results scraper):**
```python
# In _parse_match_table():
id_a = player_a["wta_id"] or name_a.lower().replace(" ", "-")
id_b = player_b["wta_id"] or name_b.lower().replace(" ", "-")
sorted_ids = sorted([id_a, id_b])
external_id = f"{year}_{tournament_id}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"
```

**Required for draw/fixtures:**
Must use same format. Key considerations:
- Use WTA ID (numeric) when available
- Fallback to normalized name if no ID
- Always sort IDs alphabetically
- Use same `tournament_id` (slug, not number)

---

## Key Differences: ATP vs WTA

| Aspect | ATP | WTA |
|--------|-----|-----|
| Tournament ID | Slug + number in URL | Numeric `tournament_number` required |
| Player ID format | Alphanumeric (`S0AG`) | Numeric (`319001`) |
| Player ID field | `atp_id` | `wta_id` |
| Draw container | `div.draw` per round | `div[data-event-type='LS']` for singles |
| Schedule URL | `/daily-schedule` | `/order-of-play` |
| Results URL | `/results` | `/scores` |
| Day navigation | Accordion headers | Button tabs with `data-date` |
| Cookie consent | Cloudflare challenge | OneTrust popup |

---

## Testing Strategy

### Test 1: Draw Scraping
```python
# scripts/test_wta_draw.py
async with WTAScraper() as scraper:
    entries = await scraper.scrape_tournament_draw(
        "abu-dhabi", 2026, tournament_number="2088"
    )
    # Verify:
    # - Correct number of entries per round
    # - Player IDs extracted (numeric)
    # - Seeds parsed correctly
    # - Byes detected
    # - Completed matches have scores/winners
```

### Test 2: Schedule Scraping
```python
# scripts/test_wta_schedule.py
async with WTAScraper() as scraper:
    async for fixture in scraper.scrape_fixtures(
        "abu-dhabi", 2026, tournament_number="2088"
    ):
        # Verify:
        # - scheduled_date populated
        # - scheduled_time where available
        # - court names extracted
        # - Player IDs match draw format
```

### Test 3: Pipeline Integration
```python
# Test that external IDs match across all three stages
draw_entries = await scraper.scrape_tournament_draw(...)
fixtures = [f async for f in scraper.scrape_fixtures(...)]
results = [m async for m in scraper.scrape_tournament_results(...)]

# For a completed match, its external_id should appear in all three
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/teelo/scrape/wta.py` | Add `scrape_tournament_draw()`, implement `scrape_fixtures()` |
| `src/teelo/db/models.py` | Verify `wta_id` field on Player model |
| `scripts/test_wta_draw.py` | Create test script |
| `scripts/test_wta_schedule.py` | Create test script |

---

## Existing Code to Reuse

The WTA scraper already has helper methods that can be reused:

| Method | Purpose | Reuse In |
|--------|---------|----------|
| `_parse_tournament_card()` | Parse tournament info | N/A (complete) |
| `_extract_player_from_row()` | Extract player name/ID/nationality | Draw, Fixtures |
| `_extract_scores_from_row()` | Parse set scores | Draw |
| `_build_score_string()` | Format score string | Draw |
| `_dismiss_cookies()` | Handle OneTrust popup | All pages |
| `_select_singles_tab()` | Filter for singles | Fixtures |
| `ROUND_MAP` | Normalize round names | Draw, Fixtures |

---

## Open Questions

1. **Draw positions** - Does WTA draw page show explicit positions, or must we infer from order?
2. **Qualifying draws** - Are Q1/Q2/Q3 rounds on separate page or same draws page with filter?
3. **Doubles handling** - Should draw/fixtures methods support doubles, or singles only? - SINGLES ONLY
4. **Historical data** - How far back does WTA website have draw/schedule data?
