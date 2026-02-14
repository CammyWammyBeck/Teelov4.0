# Hourly Update Pipeline Implementation Spec

## 1. Purpose and Scope

This spec defines how to:

1. Remove live ELO processing from `update_current_events` ingestion paths.
2. Introduce a separate incremental ELO update stage.
3. Introduce an hourly orchestration entrypoint that runs all update stages in sequence.
4. Preserve correctness, idempotency, resumability, and observability.

This is an implementation plan only. It does not execute code changes.

## 2. Target End-State

### 2.1 Stage Architecture

Hourly pipeline stages:

1. `current_events_ingest`
2. `elo_incremental`
3. `player_enrichment_incremental`
4. Future stages via stage registry

### 2.2 Runtime Guarantees

1. Each stage is independently runnable.
2. Stages are idempotent.
3. Stages are resumable from checkpoints.
4. Pipeline is protected against overlapping runs.
5. Every run emits machine-readable metrics and stage outcomes.

## 3. Current Baseline (Observed)

From `logs/update_current_events.status.jsonl` analysis:

1. Worker window: ~36 minutes for 84 tasks (3 workers).
2. Ingestion dominates total time.
3. Live ELO currently executes in draw/schedule/results ingestion loops.

Primary speed objective: remove ELO from ingest critical path, then optimize ingest separately.

## 4. Non-Goals

1. No immediate redesign of scraping logic.
2. No schema rewrite of matches/players.
3. No model training or prediction refresh in first rollout.

## 5. Proposed Changes by Component

### 5.1 Remove Live ELO from Ingestion

#### Files

1. `src/teelo/services/draw_ingestion.py`
2. `src/teelo/services/results_ingestion.py`
3. `src/teelo/services/schedule_ingestion.py`

#### Required changes

1. Remove `LiveEloUpdater.from_session(session)` usage in ingest entrypoints.
2. Remove calls to:
   - `ensure_pre_match_snapshot(...)`
   - `apply_completed_match(...)`
3. Preserve all non-ELO ingestion behavior.
4. Ensure matches that become terminal are detectable by incremental ELO stage.

#### Optional compatibility flag

Add temporary guard flag for safer rollout:

- `ENABLE_LIVE_ELO_INGEST=false` by default
- If true, keep current behavior during migration window

Recommended to remove the flag after successful rollout.

### 5.2 Introduce ELO Pending Selection Strategy

#### Option A (preferred first): derived selection

No schema changes initially. ELO stage selects matches where:

1. `status` in terminal set (`completed`, `retired`, `walkover`, `default`), and
2. `elo_post_player_a` or `elo_post_player_b` is null, or
3. `elo_needs_recompute = true`

#### Option B (later): explicit marker

Add `elo_pending` boolean on `matches` for simpler queries.

Recommendation: start with Option A, add Option B only if query cost becomes meaningful.

### 5.3 New Incremental ELO Script

#### File

1. `scripts/update_elo_incremental.py`

#### Responsibilities

1. Acquire run lock.
2. Load checkpoint.
3. Query eligible terminal matches in strict order:
   - `temporal_order ASC, id ASC`
4. Process in batches.
5. Commit each batch.
6. Persist checkpoint after successful batch commit.
7. Emit summary JSON and optional JSONL events.

#### Reused logic

Use existing ELO application logic from:

1. `src/teelo/elo/live.py`

Refactor as needed so script can call ELO processor without ingestion hooks.

#### CLI proposal

1. `--batch-size 1000`
2. `--max-matches 0` (0 = no cap)
3. `--resume` (default true)
4. `--checkpoint-key elo_incremental`
5. `--metrics-json path`
6. `--status-jsonl path`
7. `--dry-run`

### 5.4 New Player Enrichment Incremental Script

#### File

1. `scripts/update_players_incremental.py`

#### Responsibilities

1. Select unenriched/recent players via deterministic query.
2. Process in bounded batches.
3. Checkpoint progress.
4. Respect rate limits/time budgets.
5. Emit summary metrics.

#### Selection strategy (initial)

1. New players since last checkpoint, plus
2. Players missing critical fields (`nationality_ioc`, `hand`, `height_cm`, etc.)

### 5.5 New Orchestrator Script

#### File

1. `scripts/run_hourly_update.py`

#### Responsibilities

1. Acquire global pipeline lock.
2. Generate `run_id`.
3. Execute configured stages in order.
4. Capture stage metrics/status.
5. Persist run + stage artifacts.
6. Exit non-zero on policy-defined failure.

#### Stage defaults

1. `current_events_ingest` required
2. `elo_incremental` required
3. `player_enrichment_incremental` optional in first rollout (configurable cadence)

#### CLI proposal

1. `--stages current_events_ingest,elo_incremental,player_enrichment_incremental`
2. `--skip-stages ...`
3. `--fail-fast` (default true)
4. `--continue-on-error` (optional override)
5. `--max-runtime-minutes 55`
6. `--metrics-json path`
7. `--status-jsonl path`
8. `--lock-timeout-seconds 30`

### 5.6 Shared Stage Framework

#### New module proposal

1. `src/teelo/tasks/stages.py`
2. `src/teelo/tasks/runtime.py`
3. `src/teelo/tasks/checkpoints.py`
4. `src/teelo/tasks/locks.py`

#### Core interfaces

1. `StageContext`
2. `StageResult`
3. `StageRunner`
4. `CheckpointStore`

Use simple typed dataclasses to keep scripts thin and consistent.

## 6. Persistence for Runs and Checkpoints

### 6.1 Initial checkpoint storage

Start with DB-backed checkpoint table for multi-host safety.

#### New table (proposed)

`pipeline_checkpoints`

1. `key` (PK, text)
2. `value_json` (jsonb)
3. `updated_at` (timestamp)

### 6.2 Run audit tables

#### New table

`pipeline_runs`

1. `id` (PK)
2. `run_id` (unique text)
3. `started_at`
4. `ended_at`
5. `status`
6. `summary_json`

#### New table

`pipeline_stage_runs`

1. `id` (PK)
2. `run_id` (FK to `pipeline_runs.run_id`)
3. `stage_name`
4. `started_at`
5. `ended_at`
6. `status`
7. `metrics_json`
8. `error_text`

These tables make hourly operations observable and debuggable.

## 7. Locking Strategy

### 7.1 Global lock

Use PostgreSQL advisory lock for orchestrator run exclusivity.

1. Acquire lock at pipeline start.
2. Abort if unavailable.
3. Release on exit.

### 7.2 Stage lock

Optional per-stage lock if stages may be run independently outside orchestrator.

## 8. Update Current Events Integration

### 8.1 Keep existing script initially

Continue using:

1. `scripts/update_current_events.py`

but in ingest-only behavior (no live ELO).

### 8.2 Invocation from orchestrator

Orchestrator calls `update_current_events.py` as subprocess with fixed args:

1. `--workers <configured>`
2. `--quiet-worker-logs`
3. `--status-jsonl <stage path>`
4. optional `--metrics-json <stage path>`

### 8.3 Config knobs

Add environment-driven tuning:

1. `UPDATE_CURRENT_EVENTS_WORKERS`
2. `UPDATE_CURRENT_EVENTS_TOURS`
3. `UPDATE_CURRENT_EVENTS_CLEAR_QUEUE` (normally false)
4. `UPDATE_CURRENT_EVENTS_EXTRA_ARGS`

## 9. Scheduling (Hourly)

### 9.1 Single scheduler entrypoint

Replace direct scheduled calls to `update_current_events.py` with:

1. `python scripts/run_hourly_update.py`

### 9.2 systemd update

#### Files

1. `docs/systemd/teelo-update.service`
2. `docs/systemd/teelo-update.timer`

Update `ExecStart` to orchestrator command.

### 9.3 Docker update

#### File

1. `docker-compose.yml`

Add/replace service command to run orchestrator.

## 10. Metrics and Logging

### 10.1 Required per-stage metrics

1. `duration_s`
2. `records_scanned`
3. `records_processed`
4. `records_skipped`
5. `records_failed`
6. stage-specific counters (e.g., ELO applied count)

### 10.2 Artifacts

1. Unified run summary JSON (one file per hourly run)
2. Optional JSONL event stream (all stages)
3. Existing stage outputs retained for compatibility

### 10.3 Path convention

Use host-persistent path (example):

1. `/opt/teelo/logs/pipeline/<YYYY>/<MM>/<DD>/<run_id>/...`

Repo-local dev path:

1. `logs/pipeline/<run_id>/...`

## 11. Migration and Rollout Plan

### Phase 1: Foundation

1. Add stage framework modules.
2. Add advisory locking helper.
3. Add checkpoint storage + migration.
4. Add pipeline run audit tables + migration.

### Phase 2: ELO decoupling

1. Remove live ELO calls from ingest services.
2. Add `update_elo_incremental.py`.
3. Validate ELO parity on test window against baseline.

### Phase 3: Orchestrator

1. Add `run_hourly_update.py`.
2. Wire stages: ingest -> ELO.
3. Add stage metrics/reporting.

### Phase 4: Player enrichment stage

1. Add `update_players_incremental.py`.
2. Integrate as optional stage.
3. Enable by config once stable.

### Phase 5: Scheduler switch

1. Change systemd/docker schedule entrypoint to orchestrator.
2. Monitor 48-72 hours.
3. Remove legacy direct scheduling paths.

## 12. Validation Plan

### 12.1 Functional

1. New matches still ingest correctly.
2. ELO stage updates all newly terminal matches.
3. Re-running stages is idempotent.
4. Failed run resumes cleanly from checkpoint.

### 12.2 Performance

1. Compare ingest-only runtime before/after decoupling.
2. Compare full hourly runtime after adding ELO stage.
3. Verify target budget:
   - ingest stage materially reduced
   - total hourly run within operational window

### 12.3 Data correctness

1. Spot-check ELO values for sampled matches vs prior logic.
2. Validate no missing ELO for terminal matches after ELO stage.
3. Validate no duplicate player creation due to new stage boundaries.

## 13. Risks and Mitigations

1. Risk: ELO drift due to ordering differences.
   - Mitigation: strict `temporal_order,id` ordering + checkpoint tests.
2. Risk: Partial stage failures leave stale states.
   - Mitigation: commit-per-batch + checkpoint-after-commit.
3. Risk: Overlapping scheduler runs.
   - Mitigation: DB advisory lock in orchestrator.
4. Risk: Player creation transaction behavior changes.
   - Mitigation: explicit tests for identity service side effects.

## 14. Test Plan (Required)

### Unit tests

1. Checkpoint read/write semantics.
2. Lock acquisition/release semantics.
3. Stage runner status transitions.

### Integration tests

1. Ingest-only stage without ELO side effects.
2. Incremental ELO stage on seeded match set.
3. Orchestrator end-to-end with simulated stage failure + resume.

### Regression tests

1. Existing ingestion tests still pass.
2. ELO recompute consistency on historical fixture set.

## 15. Concrete File-by-File Implementation List

### New files

1. `scripts/run_hourly_update.py`
2. `scripts/update_elo_incremental.py`
3. `scripts/update_players_incremental.py`
4. `src/teelo/tasks/runtime.py`
5. `src/teelo/tasks/stages.py`
6. `src/teelo/tasks/checkpoints.py`
7. `src/teelo/tasks/locks.py`
8. `tests/integration/test_hourly_pipeline.py`
9. `tests/integration/test_update_elo_incremental.py`
10. `alembic/versions/<new>_add_pipeline_run_tables.py`
11. `alembic/versions/<new>_add_pipeline_checkpoints.py`

### Modified files

1. `src/teelo/services/results_ingestion.py` (remove live ELO calls)
2. `src/teelo/services/draw_ingestion.py` (remove live ELO calls)
3. `src/teelo/services/schedule_ingestion.py` (remove live ELO calls)
4. `docs/systemd/teelo-update.service` (orchestrator entrypoint)
5. `docker-compose.yml` (orchestrator command)
6. `docs/server-setup-arch.md` (hourly orchestrator docs)
7. `docs/server-setup-docker.md` (hourly orchestrator docs)
8. `scripts/update_current_events.py` (if minor compatibility hooks are needed only)

## 16. Configuration Additions

Add to config/env:

1. `HOURLY_PIPELINE_ENABLED_STAGES`
2. `HOURLY_PIPELINE_FAIL_FAST`
3. `HOURLY_PIPELINE_MAX_RUNTIME_MINUTES`
4. `HOURLY_PIPELINE_LOCK_KEY`
5. `ELO_INCREMENTAL_BATCH_SIZE`
6. `ELO_INCREMENTAL_MAX_MATCHES`
7. `PLAYER_ENRICH_BATCH_SIZE`
8. `PLAYER_ENRICH_MAX_PLAYERS`

## 17. Operational Runbooks

### Manual run

1. Run orchestrator once:
   - `python scripts/run_hourly_update.py --metrics-json ... --status-jsonl ...`
2. Verify stage outcomes.
3. If failed, inspect `pipeline_stage_runs` + stage JSON artifacts.

### Backfill ELO catch-up

1. Run only ELO stage with large batch:
   - `python scripts/update_elo_incremental.py --batch-size 5000`
2. Repeat until no eligible matches remain.

## 18. Acceptance Criteria

1. `update_current_events` ingest path has no live ELO updates.
2. Hourly orchestrator is the single scheduled entrypoint.
3. Incremental ELO stage reaches eventual consistency for terminal matches.
4. Pipeline runs are resumable and non-overlapping.
5. Stage-level metrics are persisted and auditable.
6. Ingest stage runtime improves measurably vs baseline.
