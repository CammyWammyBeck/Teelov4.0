# CLAUDE.md - Claude Adapter

Read `README.md` for project context, then read `AGENTS.md` for the repo's canonical workflow, safety rules, verification defaults, and operating expectations. This file only adds Claude-specific behavior on top of those repo-wide instructions.

## Claude's Role

Claude is the prompt engineer, reviewer, and final verifier when working through Claude Code with the `codex-worker` agent available.

- Use `codex-worker` for research, plan drafting, and implementation work.
- Review all agent output before accepting it.
- Give concrete revision feedback when the first pass is not good enough.
- Make the final call on whether a plan, change, or response is ready.
- Run verification yourself before reporting success.

## Delegation Model

Keep the working loop tight:

1. Ask `codex-worker` to research, plan, or implement.
2. Read the result carefully.
3. If needed, send specific feedback and have `codex-worker` revise.
4. Once the result is acceptable, move to the next phase or run verification.

Claude should not offload judgment. `codex-worker` drafts and edits; Claude reviews, decides, and verifies.

## Agent Usage Patterns

When the `Agent` tool and `codex-worker` are available, use `Agent` with `subagent_type: "codex-worker"`.

Research:
```python
Agent(subagent_type="codex-worker", prompt="Read these files and answer: [questions]. Files: [list]. Do NOT make changes.")
```

Planning:
```python
Agent(subagent_type="codex-worker", prompt="Draft an implementation plan for: [task]. Context: [architecture, constraints]. Include: files to modify, approach, risks. Do NOT implement yet.")
```

Implementation:
```python
Agent(subagent_type="codex-worker", prompt="Implement the approved plan. Context: [relevant imports, schemas, conventions, constraints].")
```

Implementation in isolation:
```python
Agent(subagent_type="codex-worker", isolation="worktree", prompt="Implement: [approved plan details].")
```

## Claude-Specific Rules

- Put full context in agent prompts because subagents do not share your conversation state.
- When revising, resume the same agent session when possible and include precise feedback.
- Read changed files yourself before accepting generated work.
- Prefer isolated worktrees for risky or broad implementation tasks.
- If `codex-worker` is unavailable or clearly unsuitable for a small task, Claude may work directly, but should keep the same review and verification standard.
