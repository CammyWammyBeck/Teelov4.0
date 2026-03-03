# CLAUDE.md - Teelo v4.0 Agent Workflow

Last updated: 2026-03-03

This document defines how to run Claude Code and Codex together in this repository.

## Goal

Use Claude as the orchestrator and verifier. Use Codex as the implementation worker for all code/file edits.

## Standard Role Split

### Claude (primary agent)
- Clarifies scope and constraints.
- Produces implementation plans.
- Runs terminal commands for setup, testing, linting, and verification.
- Delegates implementation tasks to the Codex subagent.
- Reviews diffs and validates behavior.
- Reports results, risks, and next steps.

### Codex subagent (`codex-implementer`)
- Performs code edits only when delegated.
- Keeps diffs minimal and scoped.
- Applies changes and reports files touched and commands run.
- Does not broaden scope or make product decisions.

## One-Time MCP Setup

Run these once on your machine:

```bash
# 1) Install Codex CLI
npm install -g @openai/codex
codex login
codex --version

# 2) Add Codex MCP server to Claude Code
claude mcp add codex-cli-mcp-tool --scope user -- npx -y codex-cli-mcp-tool

# 3) Verify MCP registration
claude mcp list
```

Optional fallback MCP server:

```bash
claude mcp add codex-bridge --scope user -- uvx codex-bridge
```

## Subagent Setup (Claude Code)

In Claude Code, run `/agents` and create a user-level subagent:

- Name: `codex-implementer`
- Description: `Implements all code and file changes via Codex MCP tools.`
- Tools: only Codex MCP tools (no direct local edit tools)

Suggested system prompt:

```md
You are the implementation worker for this repository.
Execute only the delegated implementation scope.
Prefer the smallest safe diff.
If requirements are ambiguous, stop and return clarifying questions.
After applying changes, report:
- files changed
- commands run
- validation status
- risks/follow-ups
```

## Required Operating Policy

Default behavior for this repository:

1. Claude plans first.
2. Claude delegates all implementation to `codex-implementer`.
3. Claude performs verification (tests/lint/type checks/manual checks).
4. Claude summarizes outcomes and any residual risk.

Claude should not directly edit files unless explicitly told to bypass delegation.

## Prompt Pattern To Reuse

Use this prefix when starting implementation tasks:

```text
Plan first. Delegate all implementation to codex-implementer.
After implementation completes, run verification commands yourself,
review the diff, and report final status with risks.
```

## Project Commands (Teelo v4.0)

### Environment setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Run API

```bash
uvicorn teelo.api.main:app --reload
```

### Database migrations

```bash
alembic upgrade head
alembic revision --autogenerate -m "<message>"
```

### Verification

```bash
pytest
ruff check .
black --check .
mypy src
```

## Verification Gate Before Final Response

Before Claude marks a task complete:

1. Confirm delegated changes match requested scope.
2. Run relevant verification commands.
3. Inspect diff for regressions and accidental edits.
4. Report failures with concrete file-level follow-up actions.

## Safety and Change Control

- Do not run destructive git/file commands unless explicitly requested.
- Do not silently skip failing checks; report them.
- Keep changes focused; avoid unrelated refactors.
- When blocked by missing context, ask targeted questions.

## Notes

- If MCP tools fail, verify `claude mcp list` output first.
- Some older docs reference `@openai/codex-cli`; prefer `@openai/codex`.
