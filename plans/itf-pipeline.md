# Implementation Plan - ITF Tournament Pipeline (Draw -> Schedule -> Results)

## 1. 🔍 Analysis & Context
*   **Objective:** Implement the full "Draw -> Schedule -> Results" pipeline for ITF tournaments, matching the existing WTA and ATP patterns. This involves adding draw scraping and schedule (fixtures) scraping to the existing results scraping.
*   **Affected Files:**
    *   `src/teelo/scrape/itf.py` (Main implementation)
    *   `scripts/explore_itf_tabs.py` (New exploration script)
    *   `scripts/test_itf_draw.py` (New test script)
    *   `scripts/test_itf_schedule.py` (New test script)
*   **Key Dependencies:** `playwright`, `BeautifulSoup`, `teelo.scrape.base` (`ScrapedDrawEntry`, `ScrapedFixture`).
*   **Risks/Unknowns:**
    *   **Order of Play Location:** The URL for the schedule is not confirmed. It's likely a tab on the tournament page.
    *   **Draw Position:** Needs to be inferred from the DOM order in the draw carousel.
    *   **Upcoming Match Structure:** How upcoming matches look in the draw (empty scores vs placeholders).

## 2. 📋 Checklist
- [ ] **Step 1: Exploration** - Identify the Order of Play URL/Tab structure.
- [ ] **Step 2: Draw Scraping** - Implement `scrape_tournament_draw` in `ITFScraper`.
- [ ] **Step 3: Schedule Scraping** - Implement `scrape_fixtures` in `ITFScraper`.
- [ ] **Step 4: Verification** - Create and run test scripts for draw and schedule.

## 3. 📝 Step-by-Step Implementation Details

### Step 1: Exploration (Order of Play)
*   **Status:** ✅ Completed.
*   **Findings:**
    *   URL Pattern: `.../tournament/{slug}/{country}/{year}/{id}/order-of-play/`
    *   **Container:** `.orderop-widget-container` represents a court.
    *   **Court Name:** `.orderop-widget-container__court-name`.
    *   **Match Item:** `.orderop-widget` inside the container.
    *   **Time:** `.orderop-widget__start-time` (e.g., "Starting at 10:00", "Not before 13:00").
    *   **Date:** `.orderop-widget__date`.
    *   **Round:** `.orderop-widget__round-details`.
    *   **Players:**
        *   Team 1: `.orderop-widget__team-info--team-1`
        *   Team 2: `.orderop-widget__team-info--team-2`
        *   Name: `.orderop-widget__first-name` + " " + `.orderop-widget__last-name`.
        *   Seed: `.orderop-widget__seeding`.

### Step 2: Implement `scrape_tournament_draw`
*   **Goal:** Extract the full draw bracket into `ScrapedDrawEntry` objects.
*   **Action:**
    *   Modify `src/teelo/scrape/itf.py`.
    *   Add method `scrape_tournament_draw(self, tournament_url: str, tournament_info: dict) -> list[ScrapedDrawEntry]`.
    *   **Logic:**
        *   Reuse the carousel navigation logic from `scrape_tournament_results`.
        *   Instead of filtering for completed matches, parse **all** match widgets.
        *   Calculate `draw_position` based on the order of matches within each round container.
        *   **R32**: 1st match is pos 1, 2nd is pos 2...
        *   Populate `ScrapedDrawEntry` fields.
        *   Handle "BYE" entries (which might not be widgets, or might be widgets with one player).
    *   **Signature:**
        ```python
        async def scrape_tournament_draw(
            self,
            tournament_url: str,
            tournament_info: dict,
        ) -> list[ScrapedDrawEntry]:
        ```

### Step 3: Implement `scrape_fixtures`
*   **Goal:** Extract upcoming scheduled matches with time and court info.
*   **Action:**
    *   Modify `src/teelo/scrape/itf.py`.
    *   Implement `scrape_fixtures(self, tournament_url: str) -> AsyncGenerator[ScrapedFixture, None]`.
    *   **Logic:**
        *   Navigate to the Order of Play page (`{tournament_url}/order-of-play/`).
        *   Iterate through `.orderop-widget-container` (courts).
        *   For each match (`.orderop-widget`):
            *   Parse date and time.
            *   Parse players (skip doubles if detected via `orderop-widget__team-doubles`).
            *   Yield `ScrapedFixture` with court info.
    *   **Signature:**
        ```python
        async def scrape_fixtures(
            self,
            tournament_url: str, # Base tournament URL
        ) -> AsyncGenerator[ScrapedFixture, None]:
        ```

## 4. 🧪 Testing Strategy
*   **Manual Verification Scripts:**
    *   `scripts/test_itf_draw.py`: Scrapes a recent tournament draw and prints entries. Check for correct positions and seeds.
    *   `scripts/test_itf_schedule.py`: Scrapes a live tournament schedule (if available).
*   **Integration Tests:**
    *   Verify that `external_id` generated in Draw, Schedule, and Results stages are identical.

## 5. ✅ Success Criteria
*   `scrape_tournament_draw` returns a complete list of draw entries with correct positions (1-32 for R32).
*   `scrape_fixtures` returns upcoming matches with court and time info.
*   External IDs are consistent across the pipeline.
