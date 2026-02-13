# ELO Optimization and Backfill Operations

This document explains what each `scripts/optimise_elo.py` command combo does after the legacy `elo_ratings` layer removal.

## Recent changes

- `elo_ratings` has been removed from runtime persistence and dropped by migration `alembic/versions/20260211_180000_drop_legacy_elo_ratings.py`.
- `scripts/optimise_elo.py --write-db` was removed.
- `EloPipeline.run_full()` was removed.
- `--rebuild-live-state` is now the only backfill path and rebuilds inline ELO artifacts only.

## ELO persistence model (current)

ELO is now persisted in one live layer:

- `player_elo_states`
- `matches.elo_pre_player_a`
- `matches.elo_pre_player_b`
- `matches.elo_post_player_a`
- `matches.elo_post_player_b`
- `matches.elo_params_version`
- `matches.elo_processed_at`
- `matches.elo_needs_recompute`

There is no `elo_ratings` table anymore.

## Core behavior shared by all optimize runs

Every run of `scripts/optimise_elo.py` does this first:

1. Load completed matches via `load_matches_for_elo(session)`.
2. Load currently active params via `get_active_elo_params(session)`.
3. Build train/test split:
   - `--split-mode temporal_order` uses tail split by `--test-ratio` (default 20% test).
   - `--split-mode match_date` uses date cutoff from `--test-months`.
4. Compute baseline log-loss with:
   - default params (`EloParams()`)
   - active params from DB
5. Run Optuna trials (`--n-trials`) and select the best trial.
6. Report improvement vs default and vs active.

No DB writes happen unless you add write/activate flags.

## Command combos

## 1) Tune only (no writes)

```bash
python3 scripts/optimise_elo.py --n-trials 200
```

What happens:

- Runs optimization only.
- Prints best parameter set and metrics.
- Does not write match ELO snapshot fields.
- Does not write `player_elo_states`.
- Does not activate parameter sets.

Use when:

- You want to evaluate candidate quality before touching DB state.

## 2) Tune and activate only if better than active

```bash
python3 scripts/optimise_elo.py \
  --n-trials 200 \
  --activate-best \
  --min-improvement 0.002
```

What happens:

- Persists best params into `elo_parameter_sets`.
- Activates only if:
  - `active_log_loss - best_log_loss >= --min-improvement`
- If below threshold, activation is skipped.
- Existing match snapshots and `player_elo_states` are not rebuilt.

Use when:

- You want safer parameter rollout with a quality gate.
- You are okay with live ingestion applying new params gradually to incoming updates.

## 3) Tune, optionally activate, then rebuild full live inline ELO state

```bash
python3 scripts/optimise_elo.py \
  --n-trials 200 \
  --activate-best \
  --min-improvement 0.002 \
  --rebuild-live-state
```

What happens:

1. Optimization and optional activation run first.
2. Rebuild job starts and does a full reset/replay:
   - Deletes all `player_elo_states`.
   - Resets all ELO snapshot/meta columns on `matches`.
3. Loads completed terminal matches (`completed`, `retired`, `walkover`, `default`) with winner and `temporal_order`.
4. Processes matches in chronological order (`temporal_order`, then `id`) through `LiveEloUpdater`.
5. Rewrites:
   - per-match pre/post snapshots and metadata on `matches`
   - current state in `player_elo_states`
6. Flags any out-of-order edge cases with `matches.elo_needs_recompute = true`.

Parameter version labeling details:

- If best params were activated, rebuild tags matches with that active set name.
- If not activated, script persists a non-active `optuna-candidate-*` record and uses that version name for rebuild tagging.

Use when:

- You want historical DB state to fully match the current inline ELO pipeline behavior.
- You changed params and need deterministic full-state recompute now, not gradually over future ingestion.

## Read/Write matrix

`scripts/optimise_elo.py` command flags and side effects:

| Combo | Reads | Writes | Main intent |
| --- | --- | --- | --- |
| `--n-trials N` | Completed matches, active params | None | Evaluate parameter quality only |
| `--activate-best` | Completed matches, active params | `elo_parameter_sets` (only if threshold met) | Promote better params safely |
| `--rebuild-live-state` | Completed terminal matches ordered by `temporal_order` | `matches` ELO snapshot/meta columns, `player_elo_states` | Rebuild full inline/live ELO artifacts |

## Recommended defaults

For current architecture, default to temporal split and guarded activation:

```bash
python3 scripts/optimise_elo.py \
  --n-trials 200 \
  --split-mode temporal_order \
  --test-ratio 0.2 \
  --activate-best \
  --min-improvement 0.002 \
  --rebuild-live-state
```

## Operational cautions

- Ensure this migration is applied before running the script:
  - `alembic/versions/20260211_180000_drop_legacy_elo_ratings.py`
- Run `alembic upgrade head` first so inline ELO tables/columns exist.
- Full live rebuild can be heavy on large datasets; schedule during low-traffic windows.
- `elo_needs_recompute=true` rows indicate chronology edge cases that were intentionally not force-applied out of order.
- After rebuild, regular ingestion (`scripts/update_current_events.py`) continues incremental updates with the active params.
