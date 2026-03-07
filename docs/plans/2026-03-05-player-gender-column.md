# Player Gender Column Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `gender` column to the `players` table, populate it on player creation from scrape source, backfill existing players, and use it to replace the expensive `_build_gender_counts_subquery` in the rankings API.

**Architecture:** Add `gender` field to the `Player` SQLAlchemy model, derive gender from the raw source string in `create_player()`, write an Alembic migration that adds the column and backfills via a join through matches → tournament_editions → tournaments (which already has a `gender` column). Replace the subquery in `api_rankings` with a simple `Player.gender == gender_param` filter.

**Tech Stack:** Python, SQLAlchemy 2.0, Alembic, PostgreSQL, FastAPI

---

### Task 1: Add `gender` to the `Player` model

**Files:**
- Modify: `src/teelo/db/models.py` (class `Player`, lines ~199–244)

**Step 1: Add the mapped column**

In `class Player`, after the `hand`/`backhand`/`height_cm` fields, add:

```python
gender: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # 'men' or 'women'
```

**Step 2: Verify it imports cleanly**

```bash
source venv/bin/activate
python3 -c "from teelo.db.models import Player; print(Player.gender)"
```
Expected: `Player.gender` attribute printed, no errors.

**Step 3: Commit**

```bash
git add src/teelo/db/models.py
git commit -m "feat: add gender column to Player model"
```

---

### Task 2: Derive gender from source in `create_player()`

**Files:**
- Modify: `src/teelo/players/identity.py` (method `create_player`, ~lines 400–447)

**Step 1: Write the failing test**

Add to `tests/unit/test_identity_service.py`:

```python
@pytest.mark.parametrize("source,expected_gender", [
    ("atp", "men"),
    ("ATP", "men"),
    ("wta", "women"),
    ("wta_125", "women"),
    ("wta125", "women"),
    ("itf_men", "men"),
    ("itf-men", "men"),
    ("ITF_MEN", "men"),
    ("itf_women", "women"),
    ("itf-women", "women"),
    ("itf", None),
    ("unknown", None),
])
def test_create_player_sets_gender(db_session, source, expected_gender):
    """create_player infers gender from source string."""
    service = PlayerIdentityService(db_session)
    player_id = service.create_player(name="Test Player", source=source)
    player = db_session.query(Player).filter_by(id=player_id).one()
    assert player.gender == expected_gender
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_identity_service.py::test_create_player_sets_gender -v
```
Expected: FAIL — `AssertionError` because `player.gender` is `None` for all sources.

**Step 3: Add `_gender_from_source` helper and wire it into `create_player()`**

Add this private method to `PlayerIdentityService` (near `_normalized_source_key`):

```python
def _gender_from_source(self, source: str) -> Optional[str]:
    s = (source or "").strip().lower()
    if s in {"atp"}:
        return "men"
    if s in {"wta", "wta_125", "wta125"}:
        return "women"
    if s in {"itf_men", "itf-men"}:
        return "men"
    if s in {"itf_women", "itf-women"}:
        return "women"
    return None
```

Then in `create_player()`, after the `Player(...)` constructor call, add:

```python
player.gender = self._gender_from_source(source)
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/unit/test_identity_service.py::test_create_player_sets_gender -v
```
Expected: PASS for all parametrize cases.

**Step 5: Run full unit test suite to check for regressions**

```bash
pytest tests/unit/ -v
```
Expected: all pass.

**Step 6: Commit**

```bash
git add src/teelo/players/identity.py tests/unit/test_identity_service.py
git commit -m "feat: infer player gender from scrape source in create_player()"
```

---

### Task 3: Alembic migration — add column and backfill

**Files:**
- Create: `alembic/versions/20260305_120000_add_player_gender.py`

**Step 1: Generate migration skeleton**

```bash
alembic revision -m "add_player_gender"
```

Rename the generated file to `alembic/versions/20260305_120000_add_player_gender.py` and set its `down_revision` to `"d4e5f6a7b8c9"` (the surface elo migration).

**Step 2: Write the migration body**

```python
"""Add gender column to players and backfill from tournament data

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-03-05 12:00:00.000000+00:00
"""

from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("players", sa.Column("gender", sa.String(length=10), nullable=True))

    # Backfill: for each player, find the majority tournament gender from their matches.
    # Uses the existing tournaments.gender column (values: 'men', 'women').
    op.execute("""
        UPDATE players p
        SET gender = sub.majority_gender
        FROM (
            SELECT
                pid,
                CASE
                    WHEN men_count >= women_count THEN 'men'
                    ELSE 'women'
                END AS majority_gender
            FROM (
                SELECT
                    pid,
                    SUM(CASE WHEN t.gender = 'men' THEN 1 ELSE 0 END)   AS men_count,
                    SUM(CASE WHEN t.gender = 'women' THEN 1 ELSE 0 END) AS women_count
                FROM (
                    SELECT m.player_a_id AS pid, te.tournament_id
                    FROM matches m
                    JOIN tournament_editions te ON te.id = m.tournament_edition_id
                    UNION ALL
                    SELECT m.player_b_id AS pid, te.tournament_id
                    FROM matches m
                    JOIN tournament_editions te ON te.id = m.tournament_edition_id
                ) match_appearances
                JOIN tournaments t ON t.id = match_appearances.tournament_id
                WHERE t.gender IN ('men', 'women')
                GROUP BY pid
            ) counts
            WHERE (men_count + women_count) > 0
        ) sub
        WHERE p.id = sub.pid
    """)


def downgrade() -> None:
    op.drop_column("players", "gender")
```

**Step 3: Run the migration**

```bash
alembic upgrade head
```
Expected: completes with no errors, prints the revision ID.

**Step 4: Verify backfill spot-check**

```bash
source venv/bin/activate
python3 -c "
from teelo.db.session import SessionLocal
from teelo.db.models import Player
db = SessionLocal()
total = db.query(Player).count()
gendered = db.query(Player).filter(Player.gender.isnot(None)).count()
null_count = total - gendered
print(f'Total: {total}, backfilled: {gendered}, still null: {null_count}')
db.close()
"
```
Expected: most players backfilled; null count should be small (players with no matches).

**Step 5: Commit**

```bash
git add alembic/versions/20260305_120000_add_player_gender.py
git commit -m "feat: migration to add and backfill players.gender column"
```

---

### Task 4: Replace `_build_gender_counts_subquery` in rankings API

**Files:**
- Modify: `src/teelo/web/services/legacy_main_handlers.py` (`api_rankings`, ~lines 1442–1517)

**Step 1: Update `api_rankings` to filter on `Player.gender`**

Replace the two usages of `gender_counts` in `api_rankings`:

Before (overall surface path):
```python
gender_counts = _build_gender_counts_subquery(db)

query = (
    db.query(Player, PlayerEloState)
    .join(PlayerEloState, PlayerEloState.player_id == Player.id)
    .join(gender_counts, gender_counts.c.pid == Player.id)
)
...
if gender_param == "men":
    query = query.filter(gender_counts.c.men_matches > gender_counts.c.women_matches)
else:
    query = query.filter(gender_counts.c.women_matches > gender_counts.c.men_matches)
```

After:
```python
if resolved_surface is None:
    query = (
        db.query(Player, PlayerEloState)
        .join(PlayerEloState, PlayerEloState.player_id == Player.id)
        .filter(Player.gender == gender_param)
    )
else:
    query = (
        db.query(Player, PlayerSurfaceEloState, PlayerEloState)
        .join(PlayerSurfaceEloState, PlayerSurfaceEloState.player_id == Player.id)
        .join(PlayerEloState, PlayerEloState.player_id == Player.id)
        .filter(PlayerSurfaceEloState.surface == resolved_surface)
        .filter(Player.gender == gender_param)
    )
```

Remove the `gender_counts` join lines entirely. Also remove the two `if gender_param == "men":` filter blocks that reference `gender_counts.c.*` — they are replaced by `Player.gender == gender_param` above.

**Step 2: Verify the server starts cleanly**

```bash
uvicorn teelo.web.main:app --reload
```
Expected: startup completes, no import errors.

**Step 3: Manually test the rankings page**

Open `http://127.0.0.1:8000/rankings` in a browser. Confirm both Men and Women tabs load quickly (should be noticeably faster — no full table scan).

**Step 4: Run full unit test suite**

```bash
pytest tests/unit/ -v
```
Expected: all pass.

**Step 5: Commit**

```bash
git add src/teelo/web/services/legacy_main_handlers.py
git commit -m "perf: replace gender subquery with Player.gender column filter in rankings API"
```

---

### Task 5: Clean up `_build_gender_counts_subquery`

**Files:**
- Modify: `src/teelo/web/services/legacy_main_handlers.py`

**Step 1: Check for any remaining usages**

```bash
grep -n "_build_gender_counts_subquery" src/teelo/web/services/legacy_main_handlers.py
```
Expected: one remaining usage at line ~1003 (inside another function — check what it is).

**Step 2: Inspect and update that remaining caller**

Read the function at line ~993 that calls `_build_gender_counts_subquery`. If it also builds a rankings-style query, apply the same `Player.gender` filter replacement. If it serves a different purpose, leave it and note it.

**Step 3: If no remaining callers, delete `_build_gender_counts_subquery`**

Once all call sites are replaced, remove the entire `_build_gender_counts_subquery` function (~lines 958–990).

**Step 4: Verify server still starts**

```bash
uvicorn teelo.web.main:app --reload
```

**Step 5: Commit**

```bash
git add src/teelo/web/services/legacy_main_handlers.py
git commit -m "refactor: remove unused _build_gender_counts_subquery"
```
