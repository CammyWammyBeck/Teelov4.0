# CLAUDE.md - Teelo v4.0 Agent Workflow

Last updated: 2026-03-08

This document defines how to run Claude Code and Codex together in this repository.

## Goal

Claude is a **prompt engineer and reviewer**. Codex CLI does the heavy lifting: research, planning, and implementation. Claude's job is to craft good prompts, review outputs, request revisions, and run verification. All work flows through an iterative loop: Codex drafts, Claude reviews, Codex revises.

## Role Split

### Claude (prompt engineer / reviewer)
- Crafts precise prompts for Codex (research, planning, and implementation).
- Reviews all Codex output (always read generated/changed files).
- Provides feedback and requests revisions (iterative loop).
- Makes final accept/reject decisions on plans and code.
- Runs verification commands (pytest, ruff, black, mypy).
- Git operations (commit, push, PR creation).
- Communicates with user and reports results.

### Claude does NOT:
- Write or edit code directly (unless Codex is unavailable/broken).
- Read files for research (delegate to Codex instead).
- Write plans from scratch (Codex drafts, Claude critiques).

### Codex CLI (drafter / implementer)
- **Research**: Reads files, searches codebase, summarizes findings.
- **Planning**: Drafts implementation plans with file lists, approach, and tradeoffs.
- **Architecture**: Proposes design options with pros/cons for Claude to choose.
- **Implementation**: Creates new files, edits existing files, refactors code.
- Keeps diffs minimal and scoped.
- Does not broaden scope or make product decisions without Claude's approval.

## The Iterative Loop

Every task follows this cycle:

```
1. Claude prompts Codex (research / plan / implement)
2. Claude reviews output
3. If not satisfied → Claude gives specific feedback → Codex revises (go to 2)
4. If satisfied → Claude accepts and moves to next phase
```

Phases for a typical task:
1. **Research** — Codex explores codebase, reports findings
2. **Plan** — Codex drafts plan, Claude reviews/refines (1-3 rounds)
3. **Implement** — Codex implements approved plan, Claude reviews code
4. **Verify** — Claude runs tests/lint, Codex fixes any issues

## MCP Setup

```bash
# Install Codex CLI
npm install -g @openai/codex
codex login

# Add Codex MCP server to Claude Code
claude mcp add codex-cli-mcp-tool --scope user -- npx -y codex-cli-mcp-tool

# Verify
claude mcp list
```

## Codex CLI Usage Patterns

### For research:
```
mcp__codex-cli__codex(
  prompt="Read these files and answer the following questions: [specific questions]. Do NOT make changes.
  Files: [list files]
  Report: [what to summarize]",
  workingDirectory="/path/to/project",
  fullAuto=true
)
```

### For planning:
```
mcp__codex-cli__codex(
  prompt="Draft an implementation plan for: [feature/task description].
  Context: [relevant architecture, constraints, patterns]
  Include: files to create/modify, approach, key decisions, risks.
  Do NOT implement yet.",
  workingDirectory="/path/to/project",
  fullAuto=true
)
```

### For plan revision:
```
mcp__codex-cli__codex(
  prompt="Revise the plan based on this feedback: [Claude's feedback].
  Original plan: [paste or reference the plan]
  Do NOT implement yet.",
  workingDirectory="/path/to/project",
  fullAuto=true
)
```

### For implementation:
```
mcp__codex-cli__codex(
  prompt="Implement the following plan: [approved plan details].
  [Include full context: DB schemas, imports, conventions, signatures]",
  workingDirectory="/path/to/project",
  fullAuto=true
)
```

### Key rules:
- Always set `fullAuto=true` for sandboxed execution.
- Set `workingDirectory` to the project root or worktree.
- Include full context in prompts (DB schemas, imports, conventions) since Codex has no conversation history.
- Parallelize independent tasks (multiple Codex calls in one message).
- Always `Read` generated files to verify before proceeding.
- When revising, include Claude's specific feedback — don't just say "fix it."

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
