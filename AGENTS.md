# Teelo Agent Guide

This file is the top-level agent operating manual for Teelo v4.0. Keep it self-sufficient: agents should not need to infer workflow expectations from historical plans or tool-specific wrappers.

## Canonical Docs Model

- `AGENTS.md` is the primary agent-facing operating manual.
- `README.md` is the primary human-facing project overview and should be read early.
- `CLAUDE.md` and `gemini.md` should stay thin tool-specific adapters and defer to this file where possible.
- A reliable top-level `README.md` is required alongside `AGENTS.md`; if docs-setup work finds it missing or stale, creating or updating it is part of completing that work.
- Source-of-truth precedence is: current code and tests, then current reference docs, then historical plans/specs.
- If implemented code conflicts with an older plan, trust the implemented code unless the task explicitly says to restore behavior to a newer approved spec.

## Repo Context

### Active Project Surface

- Core application code lives under `src/teelo/`.
- Web app bootstrap lives in `src/teelo/web/main.py`.
- Feature system code lives under `src/teelo/features/`.
- Database models and session helpers live under `src/teelo/db/`.
- ELO, scraping, ML, and task orchestration each live in dedicated `src/teelo/` modules.
- Operational entrypoints mostly live under `scripts/`; treat them carefully because many can affect live data or long-running workflows.
- Primary Python tests live under `tests/`.
- Frontend JS tests also exist under `src/teelo/web/static/js/tests/`.
- `scratchpad/` is a valid temporary workspace for notes, draft plans, and helper scripts.

### Exploration Defaults

- Prefer active source and current docs first.
- Ignore noisy trees like `node_modules/`, `__pycache__/`, and compiled CSS outputs during normal exploration unless the task directly targets them.
- Local `venv` is the default development and verification environment.
- Docker and server docs are reference paths for deployment and operations, not the default day-to-day dev workflow.

## Default Agent Workflow

1. Research first.
2. Ask before editing by default.
3. Use subagents proactively when the platform supports them.
4. Prefer persistent planning/docs for most non-trivial work.
5. Stay concise while working and detailed at the end.
6. Run full verification whenever practical.

### Working Loop

- Read the root `README.md` first for project and product context.
- Then use `AGENTS.md` as the operating guide for workflow, conventions, safety rules, and repo navigation.
- Start by reading the relevant current code and docs.
- Do that startup sequence before exploring the specific code and docs for the task.
- Summarize findings and propose the intended approach before editing, unless the user explicitly asks for direct execution.
- Leave behind durable docs or plan updates for most medium or large tasks instead of keeping all reasoning in chat.
- If subagents are available, use them for research, planning, implementation, and review; still return one coherent answer.

## Verification Defaults

- Prefer the full verification suite whenever practical:
  - `pytest`
  - `ruff check .`
  - `black --check .`
  - `mypy src`
- If frontend assets are touched, run the relevant CSS/Tailwind build.
- If schema state matters, run `alembic upgrade head` in the appropriate local/dev context before validating behavior.
- Do not claim work is complete, fixed, or passing unless you actually ran the relevant checks.
- If full verification is not practical, say exactly what ran, what was skipped, and why.
- Respect the existing pytest boundary: tests that hit live external services are opt-in/manual.

## Git, Scope, And Safety Rules

### Git Defaults

- Inspecting git state for context is fine.
- Do not commit, push, create branches, or open PRs unless the user explicitly asks.
- After completing work without git actions, use this exact handoff wording: `These are all the things I changed. Would you like me to commit and push?`

### Live-Impacting Commands

- Ask before running commands that write to the real database, run operational backfills or updates, rebuild live state, or heavily hit live external services.
- Treat `scripts/` entrypoints as potentially live-impacting unless you have confirmed otherwise.

### Scope Expansion Rules

- If you notice a completely unrelated issue, mention it in the final response only.
- If you notice an unrelated issue that may impact the requested task, stop and ask before changing it.
- If there is a direct blocker or a minimal same-area change required to finish the requested task, fix it without asking first.

### Cleanup Caution

- Do not remove files, folders, or generated assets unless the user requested it or the task clearly requires it.

## Architecture Boundaries

### Web Layer Ownership

- `src/teelo/web/main.py` is bootstrap only.
- Route handlers belong in `src/teelo/web/routers/`.
- Business logic and query/serialization helpers belong in `src/teelo/web/services/`.
- Shared request/response contracts belong in `src/teelo/web/schemas/`.
- Follow `src/teelo/web/README.md` for the current web ownership rules.

### DB And Session Conventions

- SQLAlchemy models live in `src/teelo/db/models.py`.
- Scripts and background tasks should prefer `get_session()`.
- FastAPI request handlers should use `get_db()`.
- Schema changes go through Alembic.

### Feature, ELO, And Pipeline Conventions

- Preserve the existing feature architecture built around `registry.py`, `state.py`, `engine.py`, and `groups/`.
- Preserve chronological replay from pre-match state when changing feature or ELO computation.
- Keep `docs/feature-reference.md` in sync with current feature semantics.
- Keep `docs/elo-operations.md` in sync with current ELO behavior and operational guidance.
- Hourly and update orchestration primarily lives in `scripts/` and `src/teelo/tasks/`.

## Source-Of-Truth Rules

1. Code and tests.
2. Current reference docs, especially `AGENTS.md`, `README.md`, `src/teelo/web/README.md`, `docs/feature-reference.md`, and `docs/elo-operations.md`.
3. Historical plans and specs, including older docs under `docs/plans/` and superseded implementation specs.

Trust implemented code over stale plans unless the task explicitly says otherwise.

## Documentation Expectations

- Keep `README.md` reliable as the human-facing overview.
- Keep this file reliable as the agent-facing guide.
- Non-trivial work should usually leave behind updated docs, specs, or plans.
- Review and update web-boundary, feature, ELO, deployment, or operations docs when related behavior changes.
- Mention documentation updates explicitly in the final response.

## High-Value Commands

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

Future agents should usually end with:

- what changed
- changed files with concrete file references
- brief rationale for the changes
- what verification ran and the result
- any docs updated
- follow-up notes
- any unrelated issues noticed but not changed
- the git handoff question when applicable

Prefer concrete file references so the user can inspect quickly.

## Reference Docs

- `src/teelo/web/README.md` for web boundaries
- `docs/feature-reference.md` for feature semantics
- `docs/elo-operations.md` for ELO operations and rebuild behavior
- `docs/server-setup-arch.md` and `docs/server-setup-docker.md` for deployment and server reference workflows

These reference docs support the current implementation, but code and tests remain the highest authority.
