# AGENTS.md Setup Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a canonical `AGENTS.md` for this repo, add the companion top-level `README.md`, and update tool-specific adapter docs so future agents can onboard with minimal user input.

**Architecture:** Add one human-facing root overview (`README.md`) and one agent-facing operating manual (`AGENTS.md`). Keep `CLAUDE.md` and `gemini.md` as thinner tool-specific adapters that point to `AGENTS.md` while preserving platform-specific workflow rules that should remain local.

**Tech Stack:** Markdown documentation, existing repo docs under `docs/`, Python/FastAPI project workflows

**Spec:** `docs/superpowers/specs/2026-04-18-agents-md-setup-design.md`

---

## File Structure

### New files
- `AGENTS.md` — canonical agent-facing operating manual for the repo
- `README.md` — reliable human-facing overview of the project, setup, and high-value workflows

### Modified files
- `CLAUDE.md` — thin Claude-specific adapter that defers to `AGENTS.md` and preserves tool-specific workflow rules
- `gemini.md` — thin Gemini-specific adapter that defers to `AGENTS.md`

---

## Chunk 1: Canonical Docs

### Task 1: Create the top-level README.md

**Files:**
- Create: `README.md`
- Reference: `pyproject.toml`
- Reference: `src/teelo/web/README.md`
- Reference: `docs/feature-reference.md`
- Reference: `docs/elo-operations.md`

- [ ] **Step 1: Draft a concise project overview**

Include:
- what Teelo is
- primary architecture areas under `src/teelo/`
- default local development setup
- high-value commands for app run, migrations, CSS build, and verification
- a docs map pointing to deeper references

- [ ] **Step 2: Write README.md**

Add sections for:
- project overview
- stack summary
- repo map
- local setup
- high-value commands
- docs index

- [ ] **Step 3: Read README.md for clarity and completeness**

Confirm it works as a human-facing first read and does not duplicate all agent workflow rules from `AGENTS.md`.

### Task 2: Create the top-level AGENTS.md

**Files:**
- Create: `AGENTS.md`
- Reference: `README.md`
- Reference: `src/teelo/web/README.md`
- Reference: `docs/feature-reference.md`
- Reference: `docs/elo-operations.md`
- Reference: `docs/server-setup-arch.md`
- Reference: `docs/server-setup-docker.md`

- [ ] **Step 1: Translate the approved spec into a concise operating manual**

Include sections for:
- canonical docs model
- repo context and active project surface
- default agent workflow
- verification defaults
- git, scope, and safety rules
- architecture boundaries and source-of-truth rules
- documentation expectations
- high-value commands
- expected final response shape

Explicitly encode these repo-context details:
- `scratchpad/` is a valid temporary workspace
- ignore noisy trees like `node_modules/`, `__pycache__/`, and compiled CSS outputs during normal exploration unless the task targets them
- primary Python tests live under `tests/`
- frontend JS tests also exist under `src/teelo/web/static/js/tests/`
- `scripts/` contains operational entrypoints and should be treated carefully
- local `venv` is the default dev environment, while Docker/server docs are reference paths

Explicitly encode these architecture conventions:
- web-layer ownership: `src/teelo/web/main.py` is bootstrap only, route handlers belong in `src/teelo/web/routers/`, business/query helpers belong in `src/teelo/web/services/`, and shared contracts belong in `src/teelo/web/schemas/`
- DB/session conventions: SQLAlchemy models live in `src/teelo/db/models.py`, scripts/tasks should prefer `get_session()`, FastAPI handlers should use `get_db()`, and schema changes go through Alembic
- feature/ELO/pipeline conventions: feature work should preserve the `registry.py` / `state.py` / `engine.py` / `groups/` architecture, feature computation should preserve chronological replay from pre-match state, `docs/feature-reference.md` should stay in sync with feature semantics, `docs/elo-operations.md` is the ELO operations reference, and hourly/update orchestration primarily lives in `scripts/` and `src/teelo/tasks/`

- [ ] **Step 2: Encode the user-approved workflow defaults exactly**

Preserve these decisions:
- research first, then ask before editing by default
- use subagents proactively
- prefer persistent planning/docs for most non-trivial work
- concise while working, detailed at the end
- full verification whenever practical
- ask before live-impacting commands
- no git actions unless asked, then use the specific commit/push handoff wording
- scope-expansion rules: unrelated issue -> mention only; unrelated issue that may impact task -> ask first; direct blocker or minimal same-area change required to finish -> fix without asking
- cleanup caution: do not remove files, folders, or generated assets unless requested or clearly required
- source-of-truth precedence: code/tests > current reference docs > historical plans, and trust implemented code over stale plans unless the task says otherwise

- [ ] **Step 3: Read AGENTS.md for self-sufficiency and repo specificity**

Confirm it is useful even if a future agent has not yet opened deeper docs, and that it points clearly to the right reference files.

---

## Chunk 2: Tool-Specific Adapters

### Task 3: Simplify CLAUDE.md into a thin adapter

**Files:**
- Modify: `CLAUDE.md`
- Reference: `AGENTS.md`

- [ ] **Step 1: Keep only Claude-specific execution guidance**

Retain the parts that are truly Claude/tool-specific, such as:
- the codex-worker delegation model
- Claude’s review/verification role
- any tool usage patterns that do not belong in repo-wide instructions

- [ ] **Step 2: Remove duplicated repo guidance now owned by AGENTS.md**

Delete or shrink duplicated sections for:
- project commands
- generic safety rules
- repo-wide workflow expectations

- [ ] **Step 3: Add an explicit pointer to AGENTS.md**

Make it clear that Claude should read `AGENTS.md` for repo context and operating rules first, then apply Claude-specific delegation behavior from `CLAUDE.md`.

### Task 4: Simplify gemini.md into a thin adapter

**Files:**
- Modify: `gemini.md`
- Reference: `AGENTS.md`
- Reference: `CLAUDE.md`

- [ ] **Step 1: Replace the broken pointer with a valid adapter**

Since `gemini.md` currently points to a non-existent section, replace it with a short Gemini-facing instruction that:
- points to `AGENTS.md` for repo-wide context and rules
- does not depend on `CLAUDE.md` for canonical repo guidance

- [ ] **Step 2: Keep the file intentionally minimal**

Do not re-duplicate repo rules already covered by `AGENTS.md`.

---

## Chunk 3: Verification

### Task 5: Run verification and inspect changed docs

**Files:**
- Verify: `README.md`
- Verify: `AGENTS.md`
- Verify: `CLAUDE.md`
- Verify: `gemini.md`

- [ ] **Step 1: Read the changed docs end-to-end**

Check for:
- contradictions between `README.md` and `AGENTS.md`
- broken references
- instructions that contradict the approved spec
- missing conditional verification guidance for CSS build, migrations, and skipped-check reporting

- [ ] **Step 2: Run the repo verification suite if practical**

Run:
- `source venv/bin/activate && pytest`
- `source venv/bin/activate && ruff check .`
- `source venv/bin/activate && black --check .`
- `source venv/bin/activate && mypy src`

If any command is blocked or fails for unrelated reasons, record that clearly in the final summary.

- [ ] **Step 3: Summarize changes with concrete file paths**

Final summary should include:
- what changed
- docs added/updated
- verification results
- any unrelated issues noticed
- the user-approved handoff question: `These are all the things I changed. Would you like me to commit and push?`
