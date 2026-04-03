# Tournament Forecast System Spec

## Goal

Build a tournament forecast subsystem that:

1. reads the current main draw for a tournament edition
2. materialises every possible future matchup implied by the bracket
3. computes and stores full Teelo features for each scenario match
4. computes and stores a model prediction for each scenario match
5. reuses those stored scenario predictions later as real results arrive
6. computes advancement/title probabilities on demand, not pre-persisted

---

## Scope and invariants

### In scope for v1
- Single-elimination main draw only
- Supported rounds:
  - `R128`, `R64`, `R32`, `R16`, `QF`, `SF`, `F`
- Byes supported
- Partially completed tournaments supported
- Path-dependent scenario feature generation supported
- No fallback to simplified ELO-only forecasting

### Out of scope for v1
- Qualifying
- Round robin
- Mixed doubles / team events
- Historical tracking of changing public odds over time
- Precomputed/persisted player advancement summaries

### Core invariants
- `matches` remains the source of truth for actual tournament reality
- Forecast tables store hypothetical scenario structure + predictions
- Scenario predictions are invalidated only when structural inputs change
- Actual result updates should typically require only probability recomputation, not full scenario rebuild

---


## Locked v1 decisions

These decisions are locked in for the first implementation:

- Winner-only scenario updates
  - Hypothetical path state updates track that a player advanced, played another match, and who the opponent was.
  - Do not simulate fake scorelines, durations, or detailed stat lines in v1.

- Path-sensitive feature groups included in v1
  - Include path updates for:
    - fatigue / activity
    - confidence / form
    - opponent quality
    - tournament matches played / wins within the event
  - Do not attempt to support path-sensitive features that require synthetic score-derived detail in v1.

- Schedule drift does not trigger rebuilds in v1
  - Structural draw changes trigger a rebuild.
  - Actual result/status changes trigger probability recomputation.
  - Minor schedule/date drift alone does not invalidate scenario predictions.

- Failure policy is all-or-nothing in v1
  - If node generation, feature generation, or prediction fails for any required node, the forecast run is marked failed.
  - No partial/degraded public forecast output in v1.

- Correctness first, optimisation later
  - Use full path-specific node identity in v1.
  - Do not implement aggressive state/path deduplication yet.
  - Future optimisation may deduplicate nodes by canonical state fingerprint if needed.

## High-level architecture

### Existing tables/services used
- `matches`
- `tournament_editions`
- `players`
- `match_features`
- `src/teelo/draw.py`
- current feature generation / model prediction stack

### New subsystem
#### DB
- `tournament_forecast_runs`
- `tournament_forecast_nodes`

#### Services
- `src/teelo/services/tournament_forecast.py`
- `src/teelo/services/forecast_state_builder.py`
- `src/teelo/services/forecast_prediction.py`

#### Web/API
- `GET /api/tournaments/{tour}/{tournament_code}/{year}/forecast`

#### Optional CLI/admin later
- `teelo forecast build --edition-id X`
- `teelo forecast inspect --edition-id X`

---

## Data model

### `TournamentForecastRun`

Purpose: versioned forecast build record for a tournament edition.

Suggested fields:
- `id`
- `tournament_edition_id`
- `status`
  - `building | ready | failed | stale`
- `build_reason`
  - `initial | draw_change | model_change | manual_rebuild | repair`
- `structure_signature`
- `state_signature`
- `feature_set_name`
- `model_version`
- `is_active`
- `started_at`
- `completed_at`
- `error_text`

Notes:
- keep old runs for debugging
- only one active ready run per edition at a time
- when a new run becomes active, old active run is marked stale

### `TournamentForecastNode`

Purpose: one possible matchup in one bracket slot for one forecast run.

Important: a single `(round, draw_position)` may map to many nodes.

Example for `SF`, position `1`:
- Sinner vs Alcaraz
- Sinner vs Fritz
- Medvedev vs Alcaraz
- Medvedev vs Fritz

Suggested fields:
- `id`
- `forecast_run_id`
- `round`
- `draw_position`
- `player_a_id`
- `player_b_id`
- `left_parent_node_id`
- `right_parent_node_id`
- `source_match_id`
- `node_type`
  - `actual | scenario`
- `generation_depth`
- `feature_set_name`
- `prediction_model_version`
- `player_a_state_json`
- `player_b_state_json`
- `features_json`
- `prediction_a`
- `predicted_at`
- `created_at`

Notes:
- `player_a_id` / `player_b_id` must preserve bracket side orientation, not alphabetical ordering
- `left_parent_node_id` and `right_parent_node_id` refer to the specific parent nodes whose winners could feed this node
- storing `player_a_state_json` and `player_b_state_json` is what allows path-dependent future feature generation

### Node uniqueness

Because path-dependent state can change features, node identity should include parent node references, not just players + slot.

Recommended uniqueness key:
- `forecast_run_id`
- `round`
- `draw_position`
- `player_a_id`
- `player_b_id`
- `left_parent_node_id`
- `right_parent_node_id`

---

## State model

### Why state JSON is required
A future `SF` or `F` prediction may depend on:
- earlier hypothetical wins in the same event
- tournament-specific match counts
- recent form/fatigue/activity features
- any rolling aggregates that would change if the player advanced

So child scenario nodes need access not just to player IDs, but to scenario-adjusted player state.

### Proposed state contract
Create a typed structure in Python, persisted as JSONB.

Conceptual structure:
- `player_id`
- `tournament_edition_id`
- `current_round`
- `scenario_match_count`
- `scenario_win_count`
- `synthetic_history`
- `aggregates`

Suggested persisted JSON shape:

```json
{
  "player_id": 123,
  "edition_id": 456,
  "current_round": "QF",
  "scenario": {
    "matches_played_this_event": 2,
    "wins_this_event": 2
  },
  "synthetic_recent_matches": [
    {
      "round": "R32",
      "opponent_id": 999,
      "won": true
    },
    {
      "round": "R16",
      "opponent_id": 888,
      "won": true
    }
  ],
  "feature_overrides": {
    "fatigue_matches_last_7d": 4,
    "confidence_streak": 3
  }
}
```

Rule:
- do not persist every raw feature independently unless needed
- persist the scenario-adjusted state from which features can be deterministically rebuilt
- in v1, state updates are winner-only; do not invent synthetic scorelines, durations, or detailed box-score stats for hypothetical matches

---

## Service responsibilities

### `tournament_forecast.py`
Top-level orchestration for building and querying forecast runs.

Public responsibilities:
- build a forecast run for an edition
- return the active run
- compute probabilities on demand
- detect if the draw structure changed and invalidate/rebuild when needed

Responsibilities:
- load edition + draw
- compute structure/state signatures
- reuse existing active run if valid
- create new run if needed
- materialise nodes round-by-round
- call prediction/state builders
- expose probability computation

### `forecast_state_builder.py`
Build scenario-adjusted player states and derive child winner states.

Responsibilities:
- create initial state for first-round entrants
- derive pre-match scenario state for a node from its parent winner paths
- derive winner post-match state for child generation

Important implementation note:
- this module may need feature-engine refactoring support so that scenario state can be injected into feature computation rather than relying only on DB history

### `forecast_prediction.py`
Generate features and predictions for forecast nodes.

Responsibilities:
- reuse real match prediction if a node maps to an existing unresolved `matches` row with valid stored prediction
- otherwise generate model-ready features
- run prediction model
- return and persist `features_json`, `prediction_a`

Hard requirement:
- no ELO-only fallback path
- if feature generation fails, the node/run should fail loudly rather than silently degrade
- v1 failure policy is all-or-nothing: if any required node cannot be generated or predicted, mark the run failed

---

## Signatures

### Purpose
Detect when the forecast graph must be rebuilt versus when probabilities can just be recomputed.

### Store two signatures
#### `structure_signature`
Includes:
- round
- draw_position
- players in slots
- draw topology / byes
- feature set name
- model version

Use this to determine whether nodes must be rebuilt.

#### `state_signature`
Includes:
- actual winners and statuses as well

Use this to determine whether probability recomputation is needed. Minor schedule/date drift alone does not invalidate scenario predictions in v1.

### Recommendation
- structure change => rebuild nodes + predictions
- state-only change => reuse nodes/predictions, recompute probabilities only

---

## Node generation algorithm

### Build order
Generate nodes round by round from earliest to latest using existing draw progression helpers.

### First-round node generation
Inputs:
- actual draw slots from `matches` for the edition’s first main draw round

Cases:

#### Case A — completed actual match
Create one `actual` node:
- `source_match_id = match.id`
- can reuse real features/prediction if present
- no probability stored on the node itself; probability is computed later

#### Case B — unresolved actual match with both players known
Create one `actual` node:
- same as above
- prediction must exist or be generated

#### Case C — bye
Do not create a node.
Represent deterministic advancement of that player in child generation / probability computation.

First-round nodes have:
- `left_parent_node_id = None`
- `right_parent_node_id = None`
- `generation_depth = 0`

### Later-round node generation
For each target slot `(round_code, draw_position)`:
1. find feeder slots from previous round via `get_feeder_positions`
2. enumerate all possible advancing players from left feeder
3. enumerate all possible advancing players from right feeder
4. create one node per valid cross-product matchup
5. compute pre-match scenario state for each side using the specific parent path
6. compute/store features + prediction

---

## Probability computation algorithm

Probabilities are computed on demand, not stored.

### Inputs
- active forecast run
- all forecast nodes for the run
- current actual match outcomes from `matches`

### Outputs
Compute:
- per slot: probability each player occupies it
- per player: probability of reaching each round
- per player: title probability

### Runtime maps
Build helper maps such as:
- `nodes_by_slot[(round, draw_position)] -> list[node]`
- `winner_mass_by_slot[(round, draw_position)] -> {player_id: prob}`
- `occupancy_mass_by_slot[(round, draw_position)] -> {player_id: prob}`

Semantics:
- occupancy in `QF` means player reaches QF
- winner mass in `QF` means player reaches SF

### First-round advancement masses
For each first-round source slot:
- if actual match completed: winner advances with probability `1.0`
- if actual unresolved: use stored node prediction
  - A advances with `prediction_a`
  - B advances with `1 - prediction_a`
- if bye: bye player advances with probability `1.0`

This creates:
- `advancement_probs[(round, draw_position)][player_id] = probability of winning that slot`

### Later-round propagation
For each later slot `(round, draw_position)`:
1. collect all forecast nodes for that slot
2. for each node:
   - determine probability `player_a` emerges from left feeder path
   - determine probability `player_b` emerges from right feeder path
   - node occurrence probability is the product of those two masses
3. apply stored `prediction_a`
4. accumulate winner mass into the next-round feeder distribution

For node `N`:
- `p_occurs = p_left(player_a from left parent path) * p_right(player_b from right parent path)`
- `p_a_advances = p_occurs * prediction_a`
- `p_b_advances = p_occurs * (1 - prediction_a)`

### Player round summaries
Derived on demand from slot occupancy maps:
- `reach_R64` = sum of occupancy mass across all `R64` slots
- `reach_R32` = sum of occupancy mass across all `R32` slots
- `reach_R16` = sum of occupancy mass across all `R16` slots
- `reach_QF` = sum of occupancy mass across all `QF` slots
- `reach_SF` = sum of occupancy mass across all `SF` slots
- `reach_F` = sum of occupancy mass across all `F` slots
- `win_title` = winner mass from the final slot

---

## Feature engine integration

### Requirement
The existing feature pipeline must be callable in a way that supports scenario state.

### Preferred design
Refactor feature generation into a reusable function along the lines of:
- `build_match_features(player_a_id, player_b_id, edition_id, round_code, match_date, player_a_state_override=None, player_b_state_override=None)`

Interpretation:
- with no overrides => normal DB-backed historical features
- with overrides => scenario-aware features

Reason:
- avoids duplicating the feature stack specifically for forecasts

---

## Reuse of actual match features/predictions

Rule:
If a forecast node maps to a real unresolved `matches` row:
- reuse prediction if:
  - feature set matches
  - model version matches
  - prediction exists

If prediction missing or stale:
- recompute using the normal match feature flow

Benefit:
- no redundant computation for known current matches

---

## Byes

Representation:
- do not create fake bye match forecast nodes unless absolutely necessary
- treat byes as deterministic player advancement in feeder logic

Helper logic should understand:
- real match
- completed winner
- unresolved actual node
- bye auto-advance

---

## API contract

### Route
- `GET /api/tournaments/{tour}/{tournament_code}/{year}/forecast`

### Response shape

```json
{
  "has_forecast": true,
  "forecast_run": {
    "id": 42,
    "status": "ready",
    "structure_signature": "...",
    "state_signature": "...",
    "feature_set_name": "v3_live",
    "model_version": "2026-04-01T10:22:11",
    "generated_at": "2026-04-03T03:45:00Z"
  },
  "players": [
    {
      "player_id": 1,
      "name": "Jannik Sinner",
      "seed": 1,
      "reach_r16": 0.8621,
      "reach_qf": 0.7024,
      "reach_sf": 0.4831,
      "reach_f": 0.3018,
      "win_title": 0.1794
    }
  ],
  "slots": {
    "QF": [
      {
        "draw_position": 1,
        "players": [
          {"player_id": 1, "probability": 0.61},
          {"player_id": 2, "probability": 0.39}
        ]
      }
    ]
  },
  "warnings": []
}
```

For v1:
- `slots` can be omitted if payload size becomes annoying

---

## Build / recompute workflows

### Full build
Used when:
- no active run exists
- structure signature changes
- feature/model version changes
- manual rebuild requested

Method conceptually:
- `build_forecast_run(edition_id, force=False, build_reason="initial")`

Steps:
1. load edition + draw
2. compute structure/state signatures
3. if reusable active run exists and `force=False`, return it
4. create run row (`building`)
5. create first-round actual nodes
6. create later-round scenario nodes
7. compute/store state for all nodes
8. compute/store features for all nodes
9. compute/store predictions for all nodes
10. mark run `ready`, active

### Probability recompute only
Used when:
- actual results changed
- structure unchanged

Method conceptually:
- `compute_probabilities(edition_id, run_id=None)`

Steps:
1. load active run
2. load source `matches` outcomes
3. traverse graph
4. compute per-slot/player masses
5. return result object

No DB write required unless diagnostic logging is desired.

---

## Developer task breakdown

### Task 1 — DB schema
Files:
- `src/teelo/db/models.py`
- migration file

Deliverables:
- `TournamentForecastRun`
- `TournamentForecastNode`

### Task 2 — signatures + run lifecycle
File:
- `src/teelo/services/tournament_forecast.py`

Deliverables:
- structure signature
- state signature
- active run lookup
- create/stale/fail/ready transitions

### Task 3 — first-round node materialisation
Deliverables:
- build nodes from actual draw
- map real matches to source nodes
- bye handling strategy

### Task 4 — later-round scenario node expansion
Deliverables:
- enumerate cross-product matchup nodes
- parent references
- path-safe uniqueness

### Task 5 — scenario state builder
File:
- `src/teelo/services/forecast_state_builder.py`

Deliverables:
- initial state
- node input state generation
- winner-state derivation

### Task 6 — feature/prediction integration
File:
- `src/teelo/services/forecast_prediction.py`

Deliverables:
- reuse actual predictions when possible
- build features for scenario nodes
- predict scenario nodes
- persist `features_json`, `prediction_a`

### Task 7 — probability computation engine
File:
- `src/teelo/services/tournament_forecast.py`

Deliverables:
- slot occupancy computation
- player round probabilities
- title probability

### Task 8 — API endpoint
File:
- `src/teelo/web/routers/tournaments.py`

Deliverables:
- `/forecast` endpoint
- JSON response formatting
- lazy build/rebuild trigger policy decision

### Task 9 — tests
Files:
- `tests/services/test_tournament_forecast.py`
- maybe `tests/services/test_forecast_state_builder.py`

Deliverables:
- graph correctness
- probability sums
- draw change invalidation
- result-only recompute path
- path-dependent feature correctness

---

## Recommended rollout sequence

### Phase 1
- schema
- run lifecycle
- graph generation only
- tests on bracket structure

### Phase 2
- node-level feature/prediction persistence
- real match reuse
- scenario prediction generation

### Phase 3
- probability computation
- API endpoint

### Phase 4
- UI surface on tournament page

### Phase 5
- rebuild hooks on draw updates / model changes

---

## Main implementation risk

The hardest part is not the probability maths.
It is making feature generation work correctly for hypothetical, path-dependent states.

That should drive the implementation order:
1. get the graph model right
2. refactor feature generation so scenario state can be injected cleanly
3. only then wire prediction + probabilities on top

---

## Practical recommendation

Suggested build order:
1. add `TournamentForecastRun` + `TournamentForecastNode`
2. get scenario graph generation correct
3. refactor feature generation so scenario state can be injected
4. generate/store all scenario predictions
5. compute advancement/title probabilities on demand
6. expose tournament forecast API

Most likely pain point:
- not the bracket maths
- the feature engine integration for hypothetical path-dependent states

Planned v1 optimisation stance:
- keep path-specific correctness first
- add state-fingerprint-based deduplication only later if node counts become a real problem
