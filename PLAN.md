# Backfill Historical Data Performance Optimization Plan

## Executive Summary

The `backfill_historical.py` script is slow because it processes tournaments **sequentially** with a single browser instance, and each tournament requires multiple page loads with mandatory delays. For ~50 years of data across 6 tours (ATP, Challenger, WTA, WTA 125, ITF Men, ITF Women), this could take months at current speeds.

**Key insight**: The bottlenecks are fundamentally architectural - the current design is built for reliability and correctness, not speed. To backfill 50 years quickly, we need parallel processing at multiple levels.

---

## Identified Bottlenecks (Priority Order)

### 1. **CRITICAL: Sequential Tournament Processing**
**Impact: ~95% of total time**
**Location**: `backfill_historical.py:process_queue()` (lines 257-350)

The main loop processes one tournament at a time:
```python
while True:
    task = queue_manager.get_next_task()  # Get ONE task
    # ... process entire tournament ...
    # ... commit ...
    # ... next tournament ...
```

Each tournament scrape involves:
- Looking up tournament number (1 page load + delay)
- Getting tournament info (1 page load + delay)
- Scraping results page (1 page load + delay)

With 1-3 second delays (`scrape_delay_min`/`scrape_delay_max`), each tournament takes **minimum 6-15 seconds** even before parsing.

For ATP alone: ~70 tournaments/year × 50 years = **3,500 tournaments**
At 10s average per tournament = **~10 hours** just for ATP main tour.
Add Challenger, WTA, ITF = **weeks to months**.

### 2. **HIGH: Single Browser Instance Per Scraper**
**Impact: ~70% of serial time is waiting**
**Location**: `base.py:BaseScraper.__aenter__()` (lines 453-485)

Each scraper opens one browser, creates one context. Browser operations are inherently blocking - while one page waits for network response, nothing else happens.

### 3. **HIGH: Page Loads Per Tournament Number Lookup**
**Impact: 1-2 extra page loads per tournament**
**Location**: `atp.py:_get_tournament_number()` (lines 391-433)

Every tournament without a cached number requires loading the entire archive page just to find a tournament number. For 3,500 ATP tournaments, this is **3,500 extra page loads**.

### 4. **MEDIUM: Player Matching Fuzzy Search**
**Impact: O(n) scan per player**
**Location**: `identity.py:_fuzzy_search()` (lines 524-582)

```python
aliases = self.db.query(PlayerAlias).all()  # Loads ALL aliases
for alias in aliases:
    confidence = compare_names(...)  # Compare each one
```

This loads **every alias in the database** on each fuzzy search. With thousands of players and 2 players per match, this becomes slow.

### 5. **MEDIUM: Database Round-Trips**
**Impact: N queries per tournament**
**Location**: Multiple locations in `process_scraped_match()`

Each match check and player lookup causes database queries. While there's some caching (`player_cache_by_external_id`), it's only within a single tournament.

### 6. **LOW: Virtual Display Overhead**
**Impact: Minor**
**Location**: `base.py:VirtualDisplay` (lines 216-408)

Starting Xvfb, x11vnc, and noVNC adds startup overhead but not per-request overhead. Not significant for long-running jobs.

### 7. **LOW: Random Delays Are Conservative**
**Impact: 1-3 seconds per page**
**Location**: `config.py` (lines 109-116)

```python
scrape_delay_min: float = Field(default=1.0)
scrape_delay_max: float = Field(default=3.0)
```

These delays exist to avoid rate limiting, but for historical data on older tournament pages, the rate limiting risk is lower.

---

## Proposed Solutions

### Solution 1: Parallel Tournament Processing (HIGHEST IMPACT)
**Estimated speedup: 4-8x with 4-8 workers**

Run multiple independent worker processes, each with its own browser and database session. The queue system already supports this!

**Implementation approach:**
```
Option A: Multi-process with Python
- Use multiprocessing to spawn N worker processes
- Each worker runs its own event loop + browser
- Queue is already in database (ScrapeQueue), provides natural work distribution
- Workers claim tasks via get_next_task()

Option B: Multiple Script Instances
- Simply run multiple instances of backfill_historical.py --process-only
- The queue's task claiming prevents duplicates
- Simplest to implement - just run N terminals
```

**Recommended**: Option B for simplicity, Option A for better control.

### Solution 2: Tournament Number Cache (HIGH IMPACT FOR ATP)
**Estimated speedup: 2x for ATP/Challenger (eliminates half of page loads)**

Build a tournament-to-number mapping cache by scraping the archive page once, then reuse for all tournaments from that year.

**Implementation:**
1. Before processing a year's tournaments, scrape the archive page once
2. Build dict: `{tournament_id: tournament_number}`
3. Pass cached number directly to scraper
4. Already partially implemented - `task_params["tournament_number"]` exists but only populated when available from initial list parse

### Solution 3: Reduce Delays for Historical Data
**Estimated speedup: 1.5-2x**

Historical pages are:
- Not rate-limited as aggressively
- Static content (no real-time updates)
- Less monitored by bot detection

**Implementation:**
- Add `--fast-mode` flag for historical backfill
- Reduce `scrape_delay_min` to 0.3s, `scrape_delay_max` to 1.0s
- Or remove random delays entirely between pages within same tournament

### Solution 4: Player Matching Optimization
**Estimated speedup: 10-100x for player lookups**

Replace O(n) scan with database-powered trigram matching:

**Option A: pg_trgm Extension (PostgreSQL)**
```sql
CREATE EXTENSION pg_trgm;
CREATE INDEX idx_alias_trgm ON player_aliases USING gin (alias gin_trgm_ops);
-- Then: SELECT * FROM player_aliases WHERE alias % 'djokovic' ORDER BY similarity(alias, 'djokovic') DESC LIMIT 3;
```

**Option B: Pre-built Alias Hash Index**
- Load all aliases into memory at startup
- Build a blocking-based hash (first 3 letters → candidate set)
- Only fuzzy compare against small candidate set

**Option C: Skip Fuzzy for Historical (Simpler)**
- For historical backfill, trust external IDs more
- Only queue unmatched if no external_id (rare for ATP/WTA)
- Add aliases eagerly, resolve duplicates later

### Solution 5: Batch Database Operations
**Estimated speedup: 1.3-1.5x for DB-heavy phases**

Currently commits after each tournament. For large batches:
- Batch inserts every N matches (e.g., 100)
- Use `session.execute(insert(...).values([...]))` for bulk inserts
- Already using `bulk_save_objects` for queue items (good!)

### Solution 6: Headless Mode for Background Scraping
**Estimated speedup: 1.2x (reduced resource usage)**

When running parallel workers, headed browsers waste resources. The Cloudflare bypass works with headless when:
- Using playwright-stealth (already enabled)
- Not hitting aggressive rate limits

Test headless mode specifically for ATP results pages (historical data, less protected).

---

## Recommended Implementation Order

### Phase 1: Quick Wins (1-2 days, ~4x speedup)
1. **Run multiple parallel instances** - just open 4 terminals running `--process-only`
2. **Add `--fast-delays` flag** - reduce delays for historical scraping
3. **Pre-populate tournament numbers** in queue population phase

### Phase 2: Database Optimization (2-3 days, additional 2x)
4. **Enable pg_trgm** and optimize player matching
5. **Add database indexes** if not present:
   ```sql
   CREATE INDEX idx_match_external_id ON matches(external_id);
   CREATE INDEX idx_player_atp_id ON players(atp_id);
   CREATE INDEX idx_alias_player ON player_aliases(alias, source);
   ```
6. **Batch match inserts** within each tournament

### Phase 3: Architecture Improvement (3-5 days, additional 2x)
7. **Implement proper multiprocessing** with worker pool
8. **Add progress tracking** and ETA calculation
9. **Headless mode validation** for historical pages

### Expected Total Speedup

| Optimization | Individual Speedup | Cumulative |
|--------------|-------------------|------------|
| 4 parallel workers | 4x | 4x |
| Fast delays | 1.5x | 6x |
| Tournament number cache | 1.3x | 7.8x |
| pg_trgm matching | 1.2x | 9.4x |
| Batch inserts | 1.2x | 11.3x |

**Conservative estimate: 8-12x faster**

50-year backfill time:
- Current: ~2-3 months
- After Phase 1: ~2-3 weeks
- After Phase 3: ~1 week

---

## Implementation Details

### Task 1: Multi-Instance Parallelism

Modify `backfill_historical.py` to support parallel execution:

```python
# New argument
parser.add_argument(
    "--workers",
    type=int,
    default=1,
    help="Number of parallel worker instances to spawn (1 = current behavior)",
)

# In process_queue(), add task claiming
def get_next_task():
    # Add FOR UPDATE SKIP LOCKED for proper concurrent access
    task = (
        session.query(ScrapeQueue)
        .filter(...)
        .with_for_update(skip_locked=True)  # Key change
        .first()
    )
```

### Task 2: Fast Delay Mode

```python
parser.add_argument(
    "--fast",
    action="store_true",
    help="Use faster delays for historical data (less rate-limit concern)",
)

# In scraper initialization
if args.fast:
    # Override delay settings
    settings.scrape_delay_min = 0.3
    settings.scrape_delay_max = 0.8
```

### Task 3: Tournament Number Pre-fetch

Modify `populate_queue()` to always fetch and store tournament numbers:

```python
async def populate_queue(...):
    for year in years:
        # Fetch archive page once
        archive = await scraper.get_tournament_list(year)

        for tournament in archive:
            task_params = {
                ...
                "tournament_number": tournament.get("number"),  # Already present in list!
            }
```

The tournament number IS already being captured during `get_tournament_list()` - verify it's being passed through correctly.

### Task 4: pg_trgm Setup

```sql
-- One-time setup on PostgreSQL
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add trigram index
CREATE INDEX CONCURRENTLY idx_alias_trgm
ON player_aliases USING gin (alias gin_trgm_ops);
```

Then update `_fuzzy_search()`:
```python
def _fuzzy_search(self, normalized_name: str, limit: int = 3) -> list[PlayerMatch]:
    # Use database similarity search instead of loading all
    results = self.db.execute(text("""
        SELECT player_id, alias, similarity(alias, :name) as score
        FROM player_aliases
        WHERE alias % :name
        ORDER BY score DESC
        LIMIT :limit
    """), {"name": normalized_name, "limit": limit})

    return [PlayerMatch(...) for row in results]
```

---

## Monitoring & Validation

Add progress tracking:
```python
import time

class BackfillStats:
    start_time: float
    tournaments_processed: int
    matches_created: int
    current_rate: float  # tournaments/hour

    def eta_remaining(self, total_tournaments: int) -> timedelta:
        if self.current_rate == 0:
            return timedelta(days=999)
        remaining = total_tournaments - self.tournaments_processed
        hours = remaining / self.current_rate
        return timedelta(hours=hours)
```

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Rate limiting with multiple workers | Start with 4 workers, monitor for blocks |
| Database lock contention | Use `skip_locked` for queue claiming |
| Memory usage with many browsers | Limit workers based on available RAM |
| Cloudflare blocks | Keep 1 worker as fallback with full delays |
| Data integrity with parallelism | External_id unique constraint handles duplicates |

---

## Files to Modify

1. `scripts/backfill_historical.py` - Add parallel workers, fast mode
2. `src/teelo/scrape/queue.py` - Add `skip_locked` support
3. `src/teelo/players/identity.py` - Use pg_trgm for fuzzy search
4. `src/teelo/config.py` - Add fast-scrape settings
5. Database migration - Add pg_trgm extension and index
