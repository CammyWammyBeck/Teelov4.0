# AGENTS.md Setup — Design Spec

**Created**: 2026-04-18
**Status**: Draft

## Goal

Create a canonical `AGENTS.md` for this repository so future agents can onboard quickly with minimal user input, understand the project structure, and follow the repo's preferred working style.

## Canonical Docs Model

- `README.md` is the primary human-facing overview of the project.
- `AGENTS.md` is the primary agent-facing operating manual.
- Future agents should read `README.md` early for project context, then use `AGENTS.md` as the canonical working guide.
- `CLAUDE.md` and `gemini.md` should be reduced to thin tool-specific adapters that defer to `AGENTS.md` where possible, while still preserving tool- or platform-specific execution rules that cannot be generalized.
- A reliable top-level `README.md` is a required companion deliverable for this docs model, even if it is created immediately after `AGENTS.md` rather than in the same edit.

## Repo Context To Encode In AGENTS.md

### Active Project Surface

- Core application code lives under `src/teelo/`.
- Feature system code lives under `src/teelo/features/`.
- Web app bootstrap lives in `src/teelo/web/main.py`.
- Web ownership boundaries are defined in `src/teelo/web/README.md`.
- Database models and session helpers live under `src/teelo/db/`.
- ELO, features, ML, scraping, and task orchestration each live in dedicated `src/teelo/` modules.
- Operational entrypoints mostly live in `scripts/`.
- Primary Python tests live under `tests/`, and frontend JS tests also exist under `src/teelo/web/static/js/tests/`.
- `scratchpad/` is a valid temporary workspace for notes, design drafts, and helper scripts.
- During exploration, agents should prefer active source and docs first, and ignore noisy vendored/generated trees like `node_modules/`, `__pycache__/`, and compiled CSS outputs unless the task specifically targets them.

### Default Development Context

- Local `venv` is the default environment for routine development and verification.
- Docker and server docs are operational references, not the default day-to-day development path.

## Default Agent Workflow

### 1. Start With Context

- Read the root `README.md` first for project and product context.
- Use `AGENTS.md` as the operating guide for workflow, conventions, safety rules, and repo navigation.
- Explore the relevant code and docs before proposing changes.
- Do not jump straight into editing by default.

### 2. Research First, Then Ask

- For actionable requests, the default behavior is to inspect the codebase, summarize the relevant findings, and propose an approach before making edits.
- Agents should only skip the proposal step if the user explicitly asks for direct execution.

### 3. Use Subagents Proactively

- If the platform supports subagents or parallel workers, agents should use them proactively for exploration, planning, implementation, and review.
- The main agent should still provide one coherent summary to the user.
- If subagents are unavailable, agents should follow the same research -> ask/propose -> implement -> verify loop in a single thread.

### 4. Prefer Persistent Planning

- For most non-trivial work, agents should create or update a persistent design/plan/spec doc in the repo instead of keeping all planning in chat.
- Small tasks can stay lightweight, but medium and large tasks should leave behind durable reasoning.

### 5. Communication Style

- While working: concise progress updates when there is meaningful new information.
- At completion: a fuller summary with changed files, rationale, verification, and follow-up notes.

## Verification Defaults

### Verification Standard

- Prefer the full verification suite whenever practical:
  - `pytest`
  - `ruff check .`
  - `black --check .`
  - `mypy src`
- If frontend assets are touched, run the relevant Tailwind/CSS build.
- If schema state matters, run `alembic upgrade head` in the appropriate local/dev context before validating behavior.

### Evidence Before Assertions

- Agents should not claim work is complete, fixed, or passing unless they actually ran the relevant checks.
- If full verification is blocked or impractical, they should explicitly say what was run, what was skipped, and why.
- Respect the repo's existing pytest boundary: integration tests that hit live external services are opt-in/manual.

## Git, Scope, And Safety Rules

### Git Defaults

- Agents may inspect git state for context.
- Agents should not commit, push, branch, or open PRs unless the user explicitly asks.
- After completing work, the default handoff should be a direct question such as: `These are all the things I changed. Would you like me to commit and push?`

### Live-Impacting Commands Require Approval

- Ask before running commands that write to the real database, run operational backfills/updates, rebuild live state, or heavily hit live external services.
- Do not treat `scripts/` entrypoints as automatically safe just because they exist.

### Scope Expansion Rules

- Completely unrelated issue: ignore during implementation, but mention it in the final response.
- Unrelated issue that may impact the current task: stop and ask before changing it.
- Somewhat unrelated issue that will likely affect the current task: fix it without asking first only when it is a direct blocker or a minimal same-area change required to complete the requested task.

### Cleanup Caution

- Do not remove files, folders, or generated assets unless the task clearly calls for it or the user approves it.

## Architecture Boundaries And Source Of Truth Rules

### Web Boundaries

- `src/teelo/web/main.py` is bootstrap only.
- Route handlers belong in `src/teelo/web/routers/`.
- Business logic and query helpers belong in `src/teelo/web/services/`.
- Shared request/response contracts belong in `src/teelo/web/schemas/`.
- Follow `src/teelo/web/README.md` for web ownership boundaries.

### DB And Schema Conventions

- SQLAlchemy models live in `src/teelo/db/models.py`.
- Scripts and tasks should prefer `get_session()`.
- FastAPI request handlers should use `get_db()`.
- Schema changes should go through Alembic.

### Feature, ELO, And Pipeline Conventions

- Feature work should preserve the existing `registry.py` / `state.py` / `engine.py` / `groups/` architecture.
- Feature computation should preserve chronological replay from pre-match state.
- `docs/feature-reference.md` is the reference doc for current feature semantics and should be kept in sync.
- `docs/elo-operations.md` is the operational reference for current ELO behavior.
- Hourly/update orchestration primarily lives in `scripts/` and `src/teelo/tasks/`.

### Source Of Truth Precedence

1. Current code and tests
2. Current reference docs: `README.md`, `AGENTS.md`, `src/teelo/web/README.md`, `docs/feature-reference.md`, `docs/elo-operations.md`
3. Historical planning/spec docs under `docs/plans/` and older implementation specs

If an older plan conflicts with implemented code, agents should trust the implemented code unless the task is specifically about bringing behavior back in line with a newer approved spec.

## Documentation Expectations

- `README.md` should remain a reliable human-facing overview.
- Non-trivial work should usually leave behind updated docs, plans, or specs.
- Feature, ELO, deployment, and web-boundary docs should be reviewed when related behavior changes.
- Final responses should mention documentation updates explicitly.

## High-Value Commands To Include

### Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

### Run The App

```bash
uvicorn teelo.web.main:app --reload
teelo web
```

### Migrations

```bash
alembic upgrade head
```

### CSS Build

```bash
teelo css
npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify
```

### Verification

```bash
pytest
ruff check .
black --check .
mypy src
```

## Expected Final Response Shape

Future agents should usually include:

- what changed
- any docs updated
- what verification ran and the result
- any unrelated issues noticed but not changed
- a git handoff question when applicable

They should prefer concrete file references in summaries so the user can inspect quickly.

## Implementation Direction

The implementation should create a concise but comprehensive top-level `AGENTS.md` organized around the sections above. After that file exists, `CLAUDE.md` and `gemini.md` can be simplified to point at `AGENTS.md` plus any tool-specific exceptions that still need to remain local. A reliable top-level `README.md` is also part of the intended canonical docs model and should be treated as required companion work for this setup.

## Constraints

- Assume `README.md` exists and is maintained as part of the repo's canonical docs. If it does not yet exist at implementation time, creating it is part of completing this docs setup. Keep `AGENTS.md` self-sufficient enough that workflow expectations do not depend on README wording alone.
- Do not require future agents to infer working style from historical plan docs.
- Do not include instructions that encourage automatic commits, pushes, or live-impacting scripts without user approval.
