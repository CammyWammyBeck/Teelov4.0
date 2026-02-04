# Implementation Plan - Update Current Events Script

## 1. 🔍 Analysis & Context
*   **Objective:** Create a comprehensive script (`scripts/update_current_events.py`) that discovers all currently running tournaments (ATP, WTA, ITF) within a +/- 1 week window, scrapes their draws, schedules, and results, and logs the output to a text file for verification instead of updating the database directly.
*   **Affected Files:**
    *   `scripts/update_current_events.py` (New file)
*   **Key Dependencies:**
    *   `teelo.scrape.atp.ATPScraper`
    *   `teelo.scrape.wta.WTAScraper`
    *   `teelo.scrape.itf.ITFScraper`
    *   `teelo.scrape.base` (`ScrapedDrawEntry`, `ScrapedFixture`, `ScrapedMatch`)
    *   `datetime`
*   **Risks/Unknowns:**
    *   Ensuring the "current tournament" logic correctly handles all date formats across different scrapers.
    *   Scraper reliability (timeouts, etc.) when running in sequence for multiple tournaments.

## 2. 📋 Checklist
- [ ] Step 1: Create `scripts/update_current_events.py` skeleton and imports.
- [ ] Step 2: Implement `discover_current_tournaments` supporting all tours.
- [ ] Step 3: Implement `process_tournament` to run Draw -> Schedule -> Results pipeline.
- [ ] Step 4: Implement `Logger` class to write structured output to a text file.
- [ ] Step 5: Implement `main` loop and CLI arguments.
- [ ] Step 6: Verification (Dry Run).

## 3. 📝 Step-by-Step Implementation Details

### Step 1: Script Skeleton & Imports
*   **Goal:** Set up the file structure.
*   **Action:** Create `scripts/update_current_events.py` with imports from all scrapers and base classes.

### Step 2: Tournament Discovery
*   **Goal:** Find relevant tournaments for all tour types.
*   **Action:**
    *   Implement `discover_current_tournaments(tours, year, today)`
    *   Reuse logic from `ingest_current_draws.py` but extend it to loop through `ATP`, `CHALLENGER`, `WTA`, `WTA_125`, `ITF_MEN`, `ITF_WOMEN`.
    *   Instantiate the correct scraper for each tour type to fetch the calendar.
    *   Filter by date (start date within window OR ongoing).

### Step 3: Pipeline Processing
*   **Goal:** Run the 3-stage pipeline for a single tournament.
*   **Action:**
    *   Implement `process_tournament(scraper, tournament_info, logger)`
    *   **Stage 1: Draw:** Call `scraper.scrape_tournament_draw(...)`. Log entries.
    *   **Stage 2: Schedule:** Call `scraper.scrape_fixtures(...)`. Log fixtures.
    *   **Stage 3: Results:** Call `scraper.scrape_tournament_results(...)`. Log matches.
    *   Handle exceptions gracefully so one failure doesn't stop the whole script.

### Step 4: Logging Mechanism
*   **Goal:** Write structured, readable output to a file.
*   **Action:**
    *   Create `class FileLogger`:
        *   `__init__(self, filename)`: Opens file.
        *   `log_header(self, tournament_name)`: Writes a big separator.
        *   `log_draw(self, entries)`: Writes formatted draw table.
        *   `log_schedule(self, fixtures)`: Writes fixtures (Time | Court | Players).
        *   `log_results(self, matches)`: Writes results (Winner d. Loser Score).

### Step 5: Main Execution
*   **Goal:** Tie it all together.
*   **Action:**
    *   Parse args (`--tours`, `--output`).
    *   Initialize `FileLogger` (default to `scraped_updates_{date}.txt`).
    *   Run discovery.
    *   Loop through tournaments and call `process_tournament`.
    *   Print summary to console.

## 4. 🧪 Testing Strategy
*   **Manual Verification:** Run the script `python scripts/update_current_events.py --tours ATP` (and others) and inspect the generated text file.
*   **Check:** Verify that draws, schedules, and results appear for known active tournaments.

## 5. ✅ Success Criteria
*   Script runs without errors.
*   Generates a text file containing scraped data for current tournaments.
*   Includes data from all three stages (Draw, Schedule, Results) if available.
*   Supports filtering by tour type.
