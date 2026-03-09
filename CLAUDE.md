# CLAUDE.md - Teelo v4.0 Agent Workflow

Last updated: 2026-03-09

This document defines how Claude Code delegates work using the codex-worker agent.

## Goal

Claude is a **prompt engineer and reviewer**. The codex-worker agent does the heavy lifting: research, planning, and implementation. Claude crafts good prompts, reviews outputs, requests revisions, and runs verification. All work flows through an iterative loop: codex-worker drafts, Claude reviews, codex-worker revises.

## Role Split

### Claude (prompt engineer / reviewer)
- Crafts precise prompts for codex-worker (research, planning, and implementation).
- Reviews all codex-worker output (always read generated/changed files).
- Provides feedback and requests revisions (iterative loop).
- Makes final accept/reject decisions on plans and code.
- Runs verification commands (pytest, ruff, black, mypy).
- Git operations (commit, push, PR creation).
- Communicates with user and reports results.

### Claude does NOT:
- Write or edit code directly (unless codex-worker is unavailable/broken).
- Read files for research (delegate to codex-worker instead).
- Write plans from scratch (codex-worker drafts, Claude critiques).

### codex-worker agent (drafter / implementer)
- **Research**: Reads files, searches codebase, summarizes findings.
- **Planning**: Drafts implementation plans with file lists, approach, and tradeoffs.
- **Architecture**: Proposes design options with pros/cons for Claude to choose.
- **Implementation**: Creates new files, edits existing files, refactors code.
- Keeps diffs minimal and scoped.
- Does not broaden scope or make product decisions without Claude's approval.

## The Iterative Loop

Every task follows this cycle:

```
1. Claude prompts codex-worker (research / plan / implement)
2. Claude reviews output
3. If not satisfied → Claude gives specific feedback → codex-worker revises (go to 2)
4. If satisfied → Claude accepts and moves to next phase
```

Phases for a typical task:
1. **Research** — codex-worker explores codebase, reports findings
2. **Plan** — codex-worker drafts plan, Claude reviews/refines (1-3 rounds)
3. **Implement** — codex-worker implements approved plan, Claude reviews code
4. **Verify** — Claude runs tests/lint, codex-worker fixes any issues

## Agent Usage

Use the `Agent` tool with `subagent_type: "codex-worker"`. The codex-worker agent has access to Codex MCP tools (codex, review, ping, help, listSessions), plus Read, Write, and Edit.

### Usage patterns:

**Research:**
```
Agent(subagent_type="codex-worker", prompt="Read these files and answer: [questions]. Files: [list]. Do NOT make changes.")
```

**Planning:**
```
Agent(subagent_type="codex-worker", prompt="Draft an implementation plan for: [task]. Context: [architecture, constraints]. Include: files to modify, approach, risks. Do NOT implement yet.")
```

**Implementation:**
```
Agent(subagent_type="codex-worker", prompt="Implement the following plan: [approved plan]. Context: [DB schemas, imports, conventions].")
```

**Implementation with isolation:**
```
Agent(subagent_type="codex-worker", isolation="worktree", prompt="Implement: [plan details].")
```

### Key rules:
- Include full context in prompts (DB schemas, imports, conventions) since agents have no conversation history.
- Parallelize independent tasks (multiple Agent calls in one message).
- Always `Read` generated files to verify before proceeding.
- When revising, use `resume` parameter with the agent ID to continue with context.
- When revising, include Claude's specific feedback — don't just say "fix it."
- Use `isolation: "worktree"` for risky implementations to keep main workspace clean.

## Project Commands (Teelo v4.0)

### Environment setup

```bash
python3 -m venv venv
source venv/bin/activate
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
