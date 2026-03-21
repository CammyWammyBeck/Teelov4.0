# Task 12 Defaultized Feature Returns Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove insufficient-sample `None` returns and replace them with neutral defaults across the specified feature groups.

**Architecture:** Keep feature interfaces unchanged and modify only return behavior at threshold/zero-denominator branches. Preserve all metadata-based `None` logic and all existing feature names.

**Tech Stack:** Python, pytest

---

### Task 1: Update feature-group default behaviors

**Files:**
- Modify: `src/teelo/features/groups/form.py`
- Modify: `src/teelo/features/groups/elo.py`
- Modify: `src/teelo/features/groups/h2h.py`
- Modify: `src/teelo/features/groups/activity.py`
- Modify: `src/teelo/features/groups/opponent_quality.py`
- Modify: `src/teelo/features/groups/tournament_history.py`

- [ ] **Step 1: Apply minimal logic changes**
- [ ] **Step 2: Run focused unit tests and confirm failures for old `None` expectations**
- [ ] **Step 3: Update failing tests to new defaults (`0.5`, `0.0`, `1.0`)**
- [ ] **Step 4: Run full unit test command from task and verify pass**
- [ ] **Step 5: Commit with requested message**
