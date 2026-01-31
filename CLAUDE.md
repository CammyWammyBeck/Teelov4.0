# CLAUDE.md - Teelo v4.0 Project Guide

This file provides guidance for Claude Code when working with this repository.

## Project Overview

**Teelo v4.0** is a ground-up rebuild of a tennis analytics platform for match predictions, ELO rankings, and betting analysis. This is a fresh start - do NOT migrate data from Teelov3.0; instead, scrape historical data fresh.

### Core Goals
1. **Beautiful, public website** - Make tennis data accessible and viewable for everyone
2. **Robust player identity system** - Canonical player IDs with ATP/WTA/ITF cross-matching
3. **Reliable ELO rating system** - Accurate, updateable rankings as the foundation
4. **Comprehensive coverage** - ATP, WTA, Challenger, ITF (men's and women's)
5. **Clean, maintainable code** - Less bug fixing than v3.0
6. **ML predictions** - Feature store and continuous learning (later phase)

## Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Database | Cloud-hosted PostgreSQL (API-accessible) | Complex queries + ML batch reads + accessible from anywhere |
| Tech Stack | Python-first (FastAPI, Playwright, SQLAlchemy) | Single language, easier maintenance |
| Player Identity | Canonical IDs with alias table | Eliminates fragile name matching |
| Error Handling | Queue-based with retries | No silent failures |
| ML Approach | Continuous learning with drift detection | Model always improving |
| Data Source | Fresh historical scraping | v3.0 data unreliable |
| Alerts | Discord only | Simple and effective |
| Match Storage | Unified table (scheduled → completed) | No sync issues between fixtures/matches |

## Project Structure

```
teelo/
├── CLAUDE.md                   # This file - project guidance
├── ARCHITECTURE_PLAN.md        # Detailed architecture plan
├── pyproject.toml              # Dependencies (Poetry)
├── alembic/                    # Database migrations
├── src/teelo/
│   ├── config.py               # Pydantic settings
│   ├── db/                     # SQLAlchemy models, sessions, queries
│   ├── scrape/                 # Scrapers (ATP, WTA, ITF, betting)
│   │   ├── base.py             # Abstract scraper class
│   │   ├── atp.py
│   │   ├── wta.py
│   │   ├── itf.py
│   │   ├── betting/
│   │   ├── parsers/            # Score parsing, player extraction
│   │   └── queue.py            # Scrape queue management
│   ├── players/                # Player identity system
│   │   ├── identity.py         # Matching logic with review queue
│   │   └── aliases.py
│   ├── elo/                    # ELO calculation
│   ├── features/               # Feature store
│   │   ├── store.py
│   │   └── definitions/        # Individual feature modules
│   ├── ml/                     # ML pipeline
│   │   ├── training.py
│   │   ├── prediction.py
│   │   ├── monitor.py          # Performance monitoring
│   │   ├── drift.py            # Feature drift detection
│   │   └── registry.py         # Model versioning
│   ├── api/                    # FastAPI application
│   │   ├── main.py
│   │   └── routers/
│   ├── notifications/          # Discord integration
│   └── tasks/                  # Scheduled jobs
├── tests/
├── scripts/
│   ├── backfill_historical.py  # Historical data scraping
│   └── daily_update.py
└── docker/
```

## Development Commands

```bash
# Environment setup
cd /home/cammybeck/Documents/programming/Teelov4.0
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Database
alembic upgrade head                    # Run migrations
alembic revision --autogenerate -m "msg" # Create migration

# Development
uvicorn teelo.api.main:app --reload     # API server
pytest                                   # Run tests
pytest tests/unit                        # Unit tests only
pytest -x -v                             # Stop on first failure, verbose

# Scraping
python scripts/test_ao_2024.py                      # Test ATP scraper with AO 2024
python scripts/backfill_historical.py --year 2024  # Backfill year
python scripts/daily_update.py                      # Manual update cycle

# Database (Cloud PostgreSQL)
# Connect using DATABASE_URL from .env
# Or use DBeaver GUI with cloud connection string
```

## Key Database Tables

- `players` - Canonical player records with ATP/WTA/ITF IDs
- `player_aliases` - Name variations for matching
- `player_review_queue` - Unmatched players awaiting manual review
- `tournaments` - Tournament master data
- `tournament_editions` - Yearly tournament instances
- `matches` - **Unified table**: scheduled, in-progress, and completed matches (includes odds, predictions, results)
- `elo_ratings` - Historical ELO ratings
- `feature_sets` - ML feature definitions
- `match_features` - Computed features per match
- `scrape_queue` - Pending/failed scrape tasks

**Note**: Unlike v3.0, there's NO separate fixtures table. Matches flow through status lifecycle:
`scheduled` → `in_progress` → `completed`/`retired`/`walkover`/`default`/`cancelled`

## Temporal Ordering System

Matches have a `temporal_order` BigInteger column for chronological comparisons:

```python
if match_a.temporal_order < match_b.temporal_order:
    # match_a happened before match_b
```

**Format**: `YYYYMMDD_EEEEE_RR` as integer (date + edition ID + round order)

**Date fallback chain** (in `Match.update_temporal_order()`):
1. `match_date` - Actual match date
2. `scheduled_date` - Scheduled date for fixtures
3. `sibling_date` - Most recent date from same tournament/round
4. Tournament date estimation - Interpolated from tournament start/end based on round
5. `9999-12-31` - Last resort fallback

**Round ordering** (`ROUND_ORDER` in models.py):
- Q1=1, Q2=2, Q3=3, R128=10, R64=20, R32=30, R16=40, QF=50, SF=60, F=70

This is critical for ML training (only use prior matches as features) and ELO calculations.

## ATP Scraper Implementation

**Key files**:
- `src/teelo/scrape/base.py` - Base scraper with Playwright, stealth mode, retry logic
- `src/teelo/scrape/atp.py` - ATP-specific scraper

**Important details**:

1. **Cloudflare bypass**: Uses `playwright-stealth` library. Currently requires `headless=False` to reliably bypass Cloudflare detection.

2. **URL format**: ATP requires tournament NUMBER in URLs:
   ```
   /en/scores/archive/{slug}/{number}/{year}/results
   Example: /en/scores/archive/australian-open/580/2024/results
   ```
   The `_get_tournament_number()` method looks this up from the archive page.

3. **Retry logic**: `with_retry()` accepts a callable (lambda) that creates a fresh coroutine each attempt - coroutines can only be awaited once.

4. **HTML selectors** (from v3.0):
   - Match containers: `class="match"`
   - Player names: `class="name"` → `a` tag
   - ATP IDs: From href `/en/players/{name}/{ATP_ID}/overview`
   - Scores: `class="score-item"` → `span` elements

5. **Match deduplication**: Uses `external_id` with format `{year}_{tournament_id}_{round}_{match_number}`. Has unique constraint in DB.

**Test script**: `scripts/test_ao_2024.py` - Scrapes Australian Open 2024 and stores in database.

## Player Matching Rules

**CRITICAL**: Player identity is the foundation of data quality.

1. **Exact ID match** → Auto-match (ATP/WTA/ITF ID)
2. **Exact alias match** → Auto-match (case-insensitive)
3. **High fuzzy match (>0.98)** → Auto-match, add alias
4. **Lower confidence** → Add to review queue with suggestions

Review queue items show top 3 suggestions ranked by confidence.

## ML Pipeline

### Feature Store
- Features defined as modular classes in `src/teelo/features/definitions/`
- Each feature class has `compute()` and `schema()` methods
- Features versioned - can compare model performance across versions

### Continuous Learning
- `ModelPerformanceMonitor` tracks accuracy, Brier score, calibration
- Automatic retrain triggers:
  - 30-day accuracy < 62%
  - Calibration error > 0.05
  - Accuracy drift > 3% from baseline
  - 500+ new matches available
- New models only deployed if they improve on current

### A/B Testing
- `ABTestManager` supports testing new models/features
- Deterministic assignment based on fixture ID
- Statistical significance testing for results

## Current Phase

**Phase 1: Foundation** - COMPLETE
- [x] Set up project structure (pyproject.toml, folder hierarchy)
- [x] Implement SQLAlchemy database models
- [x] Set up Alembic migrations
- [x] Create player identity system with review queue
- [x] Create base scraper class (Playwright + stealth)
- [x] Build ATP scraper (working, tested with AO 2024)
- [x] Create unit tests (ELO, score parser, player matching)
- [x] Set up PostgreSQL on Linux server
- [x] Run migrations to create tables
- [x] Test with 2024 Australian Open data (239 matches, 237 players)
- [x] Implement temporal ordering system for match chronology

**Phase 2: ELO & Ratings System** - NEXT
- [ ] Scrape ATP historical data (2020-2024)
- [ ] Implement ELO calculation pipeline
- [ ] Compute initial player ELO ratings
- [ ] Validate ratings against known ATP rankings
- [ ] Set up cloud-hosted PostgreSQL database
- [ ] Migrate data to cloud DB

**Phase 3: Website** - UPCOMING
- [ ] Build FastAPI endpoints (players, matches, rankings, tournaments)
- [ ] Build web frontend for browsing data
- [ ] Player profiles with ELO history
- [ ] Match history and tournament results
- [ ] Live ELO rankings

**Phase 4: Expand Coverage & Polish**
- [ ] Build/test WTA scraper
- [ ] Build/test ITF scraper
- [ ] Build betting odds scraper
- [ ] Automated scraping schedules
- [ ] Discord alerts

**Phase 5: ML & Predictions** (later)
- [ ] Feature store implementation
- [ ] Model training pipeline
- [ ] Continuous learning system
- [ ] Prediction display on website

## Important Constraints

### DO
- Use PostgreSQL for all data storage
- Use canonical player IDs, never raw names for joins
- Add unmatched players to review queue
- Log all scraping errors with context
- Write tests for new features
- Use async/await consistently
- Send Discord alerts for important events

### DON'T
- Migrate data from v3.0 MongoDB (scrape fresh instead)
- Use fuzzy name matching for database joins
- Silently drop data on scraping failures
- Deploy models without validation against current
- Hardcode configuration values

## Code Style

### Comments
**Write plenty of comments.** The codebase owner needs to quickly understand and edit code.

- **Module docstrings** - Every file starts with a description of its purpose
- **Class docstrings** - Explain what the class does and how to use it
- **Method docstrings** - Document parameters, return values, and behavior
- **Inline comments** - Explain non-obvious logic, especially:
  - Complex algorithms or calculations
  - Business rules ("ELO K-factor varies by tournament level because...")
  - Workarounds or edge cases
  - Why something is done a certain way (not just what)

Example:
```python
def calculate_elo(self, elo_a: Decimal, elo_b: Decimal, winner: str, level: str) -> EloUpdate:
    """
    Calculate new ELO ratings after a match.

    Uses the standard ELO formula with tennis-specific K and S factors
    that vary by tournament level. Higher K means bigger swings.

    Args:
        elo_a: Player A's rating before the match
        elo_b: Player B's rating before the match
        winner: 'A' or 'B' indicating who won
        level: Tournament level (e.g., 'Grand Slam', 'ATP 250')

    Returns:
        EloUpdate with before/after ratings for both players
    """
    # Get tournament-specific constants
    # K = volatility (how much ratings change), S = spread (rating difference impact)
    k, s = self._get_constants(level)

    # Expected score formula: E_A = 1 / (1 + 10^((R_B - R_A) / S))
    # This gives probability of A winning based on rating difference
    exp_a = Decimal("1") / (1 + Decimal("10") ** ((elo_b - elo_a) / s))
    ...
```

## ELO Constants

From v3.0 optimization (can be re-optimized later):

| Level | K | S |
|-------|---|---|
| Futures/ITF | 183 | 1241 |
| Challenger | 137 | 1441 |
| ATP 250/500 | 108 | 1670 |
| Masters 1000 | 107 | 1809 |
| Grand Slam | 116 | 1428 |

## Development Environment

**Two-machine setup:**
- **Development machine**: Ubuntu Linux (with GUI) - for writing and testing code
- **Server machine**: Arch Linux (headless, terminal only) - for running scrapers and scheduled tasks

Code must work on both machines. Test on Ubuntu before deploying to Arch.

**Database**: Cloud-hosted PostgreSQL (e.g., Neon, Supabase, Railway, or Heroku Postgres). All data is stored online so it's accessible via API from any machine. No local PostgreSQL required.

**Deployment workflow:**
1. Develop and test on Ubuntu
2. Push to git
3. Pull on Arch server
4. Run scheduled tasks on Arch (scraping, predictions)
5. API can run on either machine or a cloud host

## Environment Variables

```bash
# Required
DATABASE_URL=postgresql://user:pass@cloud-host:5432/teelo  # Cloud-hosted PostgreSQL
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Optional
LOG_LEVEL=INFO
SCRAPE_HEADLESS=false  # Set to false for ATP scraper (Cloudflare bypass requires visible browser)
```

## Testing Strategy

1. **Unit tests** - ELO calculator, score parser, feature computations
2. **Integration tests** - API endpoints, database operations
3. **Scraper validation** - Check parsed data matches expected format
4. **ML tests** - Feature computation consistency, prediction pipeline

Run before committing:
```bash
pytest tests/unit
```

## Reference: Teelov3.0

The old version is in `/home/cammybeck/Documents/programming/Teelov3.0/` for reference.

Key files to reference (but NOT migrate data from):
- `teelo/scrape/scrape_data_to_sqlite.py` - Scraping logic patterns
- `teelo/models/stats.py` - 308 feature definitions
- `config/config.py` - ELO constants, feature labels
- `teelo/models/create_elo.py` - ELO calculation logic

## Common Tasks

### Add a new feature
1. Create class in `src/teelo/features/definitions/`
2. Implement `compute()` and `schema()` methods
3. Register in feature store
4. Add tests in `tests/unit/test_features.py`
5. Create new feature set version if changing production features

### Add a new scraper
1. Create class inheriting from `BaseScraper`
2. Implement `scrape_tournament_results()` and `scrape_fixtures()`
3. Add to scrape queue task types
4. Add integration tests

### Resolve player review queue
1. Check `/api/admin/review-queue`
2. For each item: match to existing player, create new, or ignore
3. System learns from resolutions (adds aliases)

## Troubleshooting

### Scraping failures
- Check `scrape_queue` table for error messages
- Tasks retry 3 times with exponential backoff
- Check Discord for alerts

### Player matching issues
- Review queue at `/api/admin/review-queue`
- Check `player_aliases` for existing variations
- Use `PlayerIdentityService.merge_players()` for duplicates

### Model performance degradation
- Check `ModelPerformanceMonitor` metrics
- Review feature drift alerts in Discord
- Consider manual retrain trigger

## Links

- Architecture Plan: `ARCHITECTURE_PLAN.md`
- v3.0 Reference: `/home/cammybeck/Documents/programming/Teelov3.0/`

# Gemini Delegation Workflow

## Overview

This project uses a **Claude-as-supervisor, Gemini-as-executor** workflow to optimize API usage:

- **Claude Code**: Plans, architects, reviews code, makes design decisions
- **Gemini CLI**: Executes implementation tasks based on Claude's detailed instructions

This workflow leverages Claude's superior project understanding and planning abilities while offloading the actual code writing to Gemini's generous free tier.

---

## When to Delegate to Gemini

### ✅ DELEGATE these tasks using `delegate_implementation`:

- Writing new service functions or modules
- Creating new React/Vue/Svelte components  
- Implementing API endpoints or routes
- Writing database queries, models, or migrations
- Adding new utility functions
- Creating test files based on specifications
- Implementing well-defined algorithms
- Writing boilerplate or repetitive code

### ❌ KEEP these tasks in Claude:

- Architecture decisions and system design
- Planning implementation approaches
- Debugging complex issues
- Code review and quality assessment
- Complex refactoring requiring deep context
- Small fixes or changes (< 20 lines)
- Security-sensitive code review
- Making decisions about trade-offs

---

## The Delegation Process

### Step 1: Plan Thoroughly (Claude)

Before delegating, Claude MUST create a detailed implementation plan including:

```markdown
- Exact function/method signatures with full type annotations
- Step-by-step implementation logic (numbered)
- Error cases and how to handle them
- Edge cases to consider
- Which existing files to reference for patterns/style
- Any specific libraries or utilities to use
```

**The quality of Gemini's output directly depends on how detailed Claude's instructions are.**

### Step 2: Delegate (Claude calls the tool)

Call `delegate_implementation` with:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `task_description` | Brief one-line summary | "Implement Elo rating calculation service" |
| `implementation_instructions` | Detailed, numbered steps | See example below |
| `relevant_files` | 2-4 files showing project conventions | `["src/services/player.ts", "src/types/index.ts"]` |
| `output_file` | Target file path | `"src/services/elo.ts"` |

### Step 3: Review (Claude)

When Gemini returns code, Claude MUST check:

- [ ] Code follows the implementation plan
- [ ] Types are correct and complete
- [ ] Error handling matches specifications
- [ ] Code style matches project conventions
- [ ] No obvious bugs or logic errors
- [ ] Imports are correct

### Step 4: Iterate or Apply (Claude)

- **If issues found**: Use `request_gemini_fix` with specific issues
- **If satisfactory**: Apply the code to the codebase

---

## Example Delegation

### Task: Implement an Elo rating calculator for Teelo

**Claude's delegation call:**

```python
delegate_implementation(
    task_description="Implement Elo rating calculation service",
    
    implementation_instructions="""
    Create src/services/elo.ts with the following functions:
    
    1. calculateExpectedScore(playerRating: number, opponentRating: number): number
       - Formula: 1 / (1 + Math.pow(10, (opponentRating - playerRating) / 400))
       - Returns a value between 0 and 1
       - Represents probability of player winning
    
    2. calculateNewRating(
         currentRating: number, 
         actualScore: number,  // 1 for win, 0.5 for draw, 0 for loss
         expectedScore: number, 
         kFactor: number = 32
       ): number
       - Formula: currentRating + kFactor * (actualScore - expectedScore)
       - Round result to nearest integer
       - kFactor defaults to 32 but should be configurable
    
    3. processMatchResult(
         winner: { id: string, rating: number },
         loser: { id: string, rating: number },
         isDraw: boolean = false
       ): { winnerNewRating: number, loserNewRating: number }
       - Calculate expected scores for both players
       - If isDraw: actualScore is 0.5 for both
       - If not draw: winner gets 1, loser gets 0
       - Return both new ratings
    
    4. Add input validation:
       - Throw error if ratings are negative
       - Throw error if kFactor is <= 0
    
    5. Export all functions
    
    Follow the code style in src/services/player.ts (error handling pattern, JSDoc comments).
    Use the PlayerRating type from src/types/index.ts if it exists, otherwise define inline.
    """,
    
    relevant_files=[
        "src/services/player.ts",
        "src/types/index.ts", 
        "src/utils/validation.ts"
    ],
    
    output_file="src/services/elo.ts"
)
```

---

## Tips for Effective Delegation

### DO:
- Include exact function signatures with types
- Number your implementation steps
- Reference specific existing files for style
- Specify error handling behavior explicitly
- Mention which patterns to follow

### DON'T:
- Give vague instructions like "implement a rating system"
- Assume Gemini knows your project conventions
- Skip error handling specifications
- Forget to specify types
- Delegate without a clear plan

---

## Troubleshooting

### Gemini returns poor quality code
→ Your instructions weren't detailed enough. Add more specifics and try again.

### Gemini doesn't follow project conventions  
→ Include more relevant_files showing your patterns, and explicitly mention "follow the style in X file"

### Gemini times out
→ The task might be too large. Break it into smaller pieces.

### Gemini CLI not working
→ Run `check_gemini_status` tool to diagnose

---

## Quick Reference

```
# Check Gemini is working
→ Use check_gemini_status tool

# Implement new feature
→ Plan thoroughly, then use delegate_implementation

# Fix issues in Gemini's code
→ Use request_gemini_fix with specific issues

# Understand complex code
→ Use gemini_explain to offload explanation
```
