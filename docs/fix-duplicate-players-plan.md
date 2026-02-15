# Fix Duplicate Player Records Across Sources

## Context

Women's tennis players end up with duplicate records when scraped from different sources. Example: "J. Pegula" (from WTA scraper, abbreviated name) and "Jessica Pegula" (from ITF scraper, full name) are created as separate `Player` records with separate ELO ratings. The user confirmed WTA/WTA 125 produces abbreviated names ("J. Pegula") while ITF produces full names ("Jessica Pegula").

**Root cause**: Three interacting failures in the player matching pipeline:

1. **Cross-source external IDs miss**: WTA uses `wta_id`, ITF uses `itf_id` — Stage 1 (`_find_by_external_id`) checks only one column per source, so cross-source lookups fail
2. **pg_trgm can't match abbreviated names**: `similarity("j. pegula", "jessica pegula") ≈ 0.35` — way below the 0.85 threshold. The Python `compare_names()` with its `abbreviated_bonus` (+0.15) would handle this, but it's bypassed when pg_trgm is active
3. **`_resolve_player()` fallback auto-creates after queuing**: When `find_or_queue_player` returns `(None, "queued")`, both ingestion services immediately call `create_player()` if an external ID exists — defeating the review queue

---

## Changes

### 1. Add abbreviated name matching (Stage 2.5) in `find_or_queue_player()`

**File**: `src/teelo/players/identity.py`

Insert a new matching stage between Stage 2 (exact alias) and Stage 3 (fuzzy):

- New method `_find_by_abbreviated_match(normalized_name)` that uses `extract_last_name()` from `aliases.py`
- **If input is abbreviated** (first part is single letter ± period, e.g. "j. pegula"):
  - Query aliases table: `alias LIKE '{initial}% {last_name}'` excluding aliases shorter than 3 chars in first part (to avoid matching other abbreviated forms)
  - E.g., `LIKE 'j% pegula'` → finds "jessica pegula"
- **If input is full name** (e.g. "jessica pegula"):
  - Query for abbreviated aliases: `alias IN ('j pegula', 'j. pegula')`
- Deduplicate candidates by `player_id`
- If exactly ONE unique player → auto-match, call `_ensure_alias()` + `_link_external_id()`
- If multiple players share the initial + last name → add to the `player_review_queue` table with all candidates as suggestions, so the admin can confirm the correct match later via an admin page on the website

Also add a small helper `is_abbreviated_name(name)` to `aliases.py`.

**Future work (not in this PR)**: Build an admin-only page on the website to review and resolve ambiguous abbreviated name matches from the review queue.

### 2. Add Python fallback to `_fuzzy_search()` when pg_trgm misses

**File**: `src/teelo/players/identity.py`, `_fuzzy_search()` method (line 565)

After the pg_trgm query, if no candidate reaches `exact_match_threshold`:
- Extract last name with `extract_last_name()`
- Query aliases sharing that last name: `alias LIKE '% {last_name}'` or `alias = '{last_name}'`
- Run Python `compare_names()` on each (which applies `abbreviated_bonus`)
- Merge these results with the pg_trgm results, keeping best score per player

This is a belt-and-suspenders defense — Stage 2.5 should catch most cases, but this ensures fuzzy suggestions in the review queue are also correct.

### 3. Make `_resolve_player()` fallback smarter

**Files**: `src/teelo/services/results_ingestion.py` (line 514), `src/teelo/services/draw_ingestion.py` (line 429)

Currently: `if not player_id and external_id: create_player()`

Change to: Before creating, call `identity_service._find_by_abbreviated_match(normalized_name)` one more time as a safety check. Only create if that also returns None. This prevents duplicates even if Stage 2.5 was somehow bypassed.

### 4. Enhance `merge_players()` to handle ELO state

**File**: `src/teelo/players/identity.py`, `merge_players()` method (line 383)

Add to the existing merge logic:
- Delete the `PlayerEloState` row for the merged player (if exists)
- Set `elo_needs_recompute = True` on all matches involving the kept player so ELO gets recalculated on next incremental run
- Log the merge for audit trail

### 5. Create duplicate detection and merge script

**New file**: `scripts/find_and_merge_duplicate_players.py`

```
Usage:
  python scripts/find_and_merge_duplicate_players.py           # Dry run (report only)
  python scripts/find_and_merge_duplicate_players.py --execute  # Merge high-confidence dupes
```

Logic:
1. Load all players with aliases
2. Group by last name using `extract_last_name()`
3. Within each group, compare all pairs using `compare_names()` (also compare across all alias pairs)
4. Report pairs scoring >= 0.85
5. With `--execute`: merge high-confidence pairs (>= 0.95) automatically using `merge_players()`, keeping the player with more matches/external IDs
6. After merges, affected matches have `elo_needs_recompute = True` (handled by the enhanced `merge_players()`)

### 6. Add tests

**File**: `tests/unit/test_player_matching.py` (extend existing)

- Test `_find_by_abbreviated_match()` with: "j. pegula" ↔ "jessica pegula", "j. del potro" ↔ "juan martin del potro", "k. pliskova" with two Pliskovas in DB (should return None)
- Test the hybrid fuzzy fallback produces correct scores for abbreviated names
- Test the full `find_or_queue_player()` flow with abbreviated cross-source scenario

---

## Files Modified

| File | Change |
|------|--------|
| `src/teelo/players/identity.py` | Add `_find_by_abbreviated_match()`, integrate Stage 2.5, fix `_fuzzy_search()` hybrid fallback, enhance `merge_players()` for ELO |
| `src/teelo/players/aliases.py` | Add `is_abbreviated_name()` helper |
| `src/teelo/services/results_ingestion.py` | Safety check in `_resolve_player()` fallback |
| `src/teelo/services/draw_ingestion.py` | Same safety check in `_resolve_player()` fallback |
| `scripts/find_and_merge_duplicate_players.py` | New dedup script |
| `tests/unit/test_player_matching.py` | New test cases |

---

## Verification

1. **Unit tests**: Run `pytest tests/unit/test_player_matching.py` to verify abbreviated matching logic
2. **Dedup script dry run**: `python scripts/find_and_merge_duplicate_players.py` — verify it correctly identifies known duplicates (J. Pegula / Jessica Pegula) without merging
3. **Dedup script execute**: `python scripts/find_and_merge_duplicate_players.py --execute` — merge duplicates, verify player count decreases
4. **ELO recalculation**: After merge, run the incremental ELO update to recompute ratings for affected players
5. **Manual verification**: Check the website — search for "Pegula" should return only one result with combined match history
