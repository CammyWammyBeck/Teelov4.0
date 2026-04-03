# Tournament Forecast Implementation Tasks

This is the concrete build order for the tournament forecast system.

## Phase 1 — Schema + run lifecycle

### 1. Add DB models + migration
Create:
- `TournamentForecastRun`
- `TournamentForecastNode`

Include:
- structure/state signatures
- active/stale/failed/ready lifecycle
- node parent references
- stored scenario state JSON
- stored features JSON
- stored prediction value/model version

### 2. Add run lifecycle service shell
Create:
- `src/teelo/services/tournament_forecast.py`

Implement:
- `get_active_run(edition_id)`
- `compute_structure_signature(...)`
- `compute_state_signature(...)`
- `build_forecast_run(..., force=False)` shell
- run status transitions: `building -> ready|failed`, old active run -> `stale`

### 3. Add tests for lifecycle/signatures
Tests:
- active run lookup works
- same structure signature reuses run
- changed structure signature creates new run
- state-only change does not require rebuild

---

## Phase 2 — Graph generation

### 4. Materialise first-round nodes
Build forecast nodes from the first playable main-draw round.

Cases:
- completed actual match -> actual node
- unresolved actual match -> actual node
- bye -> deterministic advancement, no fake node

### 5. Materialise later-round scenario nodes
For each later slot:
- enumerate valid feeder winners
- create cross-product scenario nodes
- preserve bracket-side orientation
- include parent node references in identity

### 6. Add graph correctness tests
Tests:
- 4-player draw creates expected nodes
- 8-player draw feeder structure is correct
- byes propagate correctly
- same visible matchup can exist multiple times if parent path differs

---

## Phase 3 — Scenario state builder

### 7. Add scenario state builder module
Create:
- `src/teelo/services/forecast_state_builder.py`

Implement:
- `build_initial_state(...)`
- `build_node_input_states(...)`
- `derive_winner_state(...)`

Locked v1 rules:
- winner-only scenario updates
- use simple synthetic defaults for forecast-only hypothetical match score-derived fields
- path-sensitive updates for:
  - fatigue/activity
  - confidence/form
  - opponent quality
  - tournament matches played/wins
  - score-derived groups via forecast-only synthetic defaults where needed

### 8. Lock forecast feature approximation policy
Document and implement the forecast-only synthetic defaults used for hypothetical matches.

Examples to define explicitly:
- default straight-sets style win
- fixed neutral game-count assumption
- no tiebreak
- no comeback
- no first-set-lost
- no close-match flag

Output:
- a short mapping doc/code comment listing which feature groups are static vs scenario-updated and which synthetic defaults are used

### 9. Add state-builder tests
Tests:
- initial state is built consistently
- winner-state derivation updates supported fields correctly
- synthetic forecast defaults are applied consistently for score-derived fields

---

## Phase 4 — Feature/prediction integration

### 10. Refactor feature generation for scenario overrides
Goal:
- support feature building from normal DB history + optional scenario state overrides

Implement/refactor something like:
- `build_match_features(..., player_a_state_override=None, player_b_state_override=None)`

### 11. Add forecast prediction service
Create:
- `src/teelo/services/forecast_prediction.py`

Implement:
- reuse real match prediction when valid
- otherwise build features from scenario state
- predict and persist:
  - `features_json`
  - `prediction_a`
  - `prediction_model_version`
  - `predicted_at`

### 12. Define strict reuse + reprediction rules
Only reuse actual match predictions if:
- prediction exists
- model version matches exactly
- feature assumptions are compatible

Otherwise:
- recompute forecast-local feature payload + prediction

Also enforce:
- when a matchup becomes real/known, regenerate the real match prediction through the normal live feature pipeline
- treat the real `matches` prediction as authoritative for user-facing live match odds

### 13. Add prediction integration tests
Tests:
- actual match prediction reuse works when versions match
- stale/missing prediction triggers recompute
- scenario node gets stored features + prediction
- no ELO fallback path exists

---

## Phase 5 — Probability engine

### 14. Implement probability computation
Inside `tournament_forecast.py`, compute on demand:
- slot occupancy probabilities
- player reach-round probabilities
- title probability

Do not persist these in v1.

### 15. Support result-only recomputation
When actual results change but structure does not:
- reuse stored nodes
- reuse stored predictions
- recompute probabilities only

### 16. Add probability tests
Tests:
- title probabilities sum to 1
- slot occupancy sums to 1
- completed result collapses branch probabilities correctly
- byes handled correctly

---

## Phase 6 — API

### 17. Add tournament forecast endpoint
Add:
- `GET /api/tournaments/{tour}/{tournament_code}/{year}/forecast`

Return:
- forecast run metadata
- player round probabilities
- optional slot distributions
- warnings/build status if relevant

### 18. Decide API build behaviour
Pick one:
- build lazily on first request
- require prebuild/manual trigger

Recommended for dev:
- allow lazy build initially

### 19. Add endpoint tests
Tests:
- returns `has_forecast=false` or build status cleanly when unavailable
- returns player probabilities correctly when ready
- handles failed/building states cleanly

---

## Phase 7 — UI

### 20. Add tournament page forecast section
Initial UI can be simple:
- title odds
- chance to reach QF/SF/F

### 21. Add basic loading/error states
Need to support:
- building
- ready
- failed
- no forecast yet

---

## Phase 8 — Operational hooks

### 22. Hook rebuild logic into draw changes
When structure changes:
- invalidate active run
- trigger rebuild

### 23. Hook probability recompute into result updates
When results/statuses change without structural draw changes:
- do not rebuild nodes
- recompute probabilities only

### 24. Add manual tooling
Optional but useful:
- CLI/admin command to build forecast run
- CLI/admin command to inspect active run / node counts / failures

---

## Suggested order of attack

1. Schema + run lifecycle
2. Graph generation
3. State builder
4. Feature-engine refactor
5. Prediction persistence
6. Probability engine
7. API
8. UI
9. Rebuild hooks / tooling

---

## Biggest engineering risk

The hardest part is feature-engine integration for hypothetical path-dependent states.

Not the bracket maths.

That means the safest sequence is:
- prove graph generation first
- prove scenario state shape second
- refactor feature generation third
- only then wire predictions and probabilities on top
