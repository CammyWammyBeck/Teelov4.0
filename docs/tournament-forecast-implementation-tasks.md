# Tournament Forecast Implementation Tasks


## Handoff checklist

Before implementation starts, the engineer should confirm they understand these non-negotiable rules:

- only build/apply this system for ATP Tour and WTA Tour events
- use the normal live feature set
- synthetic hypothetical scoreline default is `6-4 6-4`
- synthetic hypothetical records also assume:
  - no tiebreak
  - no comeback
  - no first-set-lost
  - no close-match flag
- hypothetical ELO updates use the same logic as real completed matches
- forecast predictions are not authoritative once a matchup becomes real
- real known matchups must be repredicted through the normal live pipeline
- forecast auto-build uses the unified draw-readiness rule
- public read API does not lazily build forecasts in v1
- probability outputs are computed on demand, not persisted
- failure policy is all-or-nothing

If any code plan conflicts with the list above, the docs win.


This is the concrete build order for the tournament forecast system.


## Final implementation decisions

These implementation policies are locked in:

1. Use the normal live feature set
- Forecast uses the same maintained feature set/model path as live Teelo predictions.
- No separate forecast-only model is introduced.
- Any forecast approximations should be handled at the scenario-state / synthetic-match layer, not by maintaining a second model.

2. Synthetic default scoreline for hypothetical matches
- Use a simple default like `6-4 6-4` for forecast-only hypothetical matches.
- This provides consistent values for score-derived fields without leaving them blank.

3. Hypothetical ELO updates use normal calculations
- When a hypothetical prior match is processed in the forecast path, update ELO exactly as if that match had completed.
- Use the same ELO update logic/calculations as the real system.

4. Forecast builds automatically once the draw is full
- Forecast should auto-build when the main draw is considered full/ready.
- In practice this is typically after qualifying is complete.
- Implementation needs a concrete readiness check to decide when a draw is full enough to build.

5. API behaviour when no forecast exists
- If a forecast is possible, it should already have been built by the automatic trigger.
- Therefore returning `not built yet` / no forecast is acceptable when no active run exists.
- The read API does not need to lazily build forecasts on request in v1.


## Draw readiness / auto-build rule

Forecast auto-build should use one unified readiness rule for all draw types.

### Unified rule
Determine the tournament's effective entry round, then inspect the round immediately after it.

A draw is forecast-ready when **every slot in the round after the entry round is resolvable**.

A slot is resolvable if both incoming paths are known via either:
- a complete feeder match in the entry round, or
- a propagated bye / auto-advance player already materialised into that next-round slot

### Why this single rule works
It handles both cases without branching into separate systems:
- non-bye draws
- bye draws

For a normal draw, every next-round slot is only resolvable once both feeder matches are fully populated.
For a bye draw, the same logic works because one side may already be known from a propagated bye path.

### Practical algorithm
1. determine the effective entry round for the edition
2. determine the round immediately after that entry round
3. iterate every slot in that next round
4. for each slot, inspect:
   - the two feeder entry-round positions
   - the next-round match row itself, if present
5. mark the slot resolvable if both incoming sides are known through feeder matches or propagated byes
6. auto-build only when all next-round slots are resolvable

### Notes / edge cases
- qualifying placeholders should not count as ready unless they resolve to real player IDs
- `TBD` / null player slots are not ready
- if ingestion has already propagated bye winners into the next round, use that as readiness evidence
- this is a structural readiness check, not a schedule check

### Tournament eligibility filter

Forecast generation must enforce tournament eligibility before doing any other work.

Allowed in v1:
- ATP Tour main-tour events
- WTA Tour main-tour events

Excluded in v1:
- Challenger
- WTA 125
- ITF

Implementation requirement:
- add a single eligibility helper (for example `is_forecast_eligible_tournament(...)`) and use it consistently everywhere
- do not duplicate ad hoc filtering rules across multiple call sites

Suggested checks:
- tournament belongs to ATP or WTA tour only
- tournament level is a main-tour forecast-eligible level
- explicitly reject Challenger / 125 / ITF even if other fields are ambiguous

Recommended usage points:
- forecast auto-build trigger
- forecast build service entrypoint
- forecast API endpoint
- any CLI/admin/manual build command

If a tournament is not eligible:
- do not build a forecast run
- API should return no forecast / not supported for that event

## Phase 1 — Schema + run lifecycle

### Scope guard
Before any build work, enforce tournament eligibility via one shared helper:
- allowed: ATP Tour, WTA Tour
- excluded: Challenger, WTA 125, ITF

This helper should be used by:
- auto-build trigger
- forecast build service
- forecast API endpoint
- any CLI/admin/manual build path


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
Also implement tournament eligibility + draw-readiness detection:
- determine whether the event is forecast-eligible (ATP/WTA main tour only)
- determine when a draw is full/ready enough to auto-build
- target behaviour is to build automatically once the full main draw is known

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
- default synthetic scoreline is `6-4 6-4` unless a later explicit rule replaces it
- path-sensitive updates for:
  - fatigue/activity
  - confidence/form
  - opponent quality
  - tournament matches played/wins
  - score-derived groups via forecast-only synthetic defaults where needed

### 8. Lock forecast feature approximation policy
Use these v1 defaults:
- same live feature set as normal Teelo predictions
- synthetic hypothetical scoreline default: `6-4 6-4`
- hypothetical ELO updates use the same completed-match calculation path as real matches

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
- return `not built yet` / equivalent empty state if no active forecast exists
- return unsupported / no forecast for ineligible events outside ATP/WTA main tour scope

Return:
- forecast run metadata
- player round probabilities
- optional slot distributions
- warnings/build status if relevant

### 18. API build behaviour
Locked v1 behaviour:
- no lazy build on public read API
- if a forecast is possible, it should already have been auto-built
- otherwise return `not built yet` / equivalent empty state

### 19. Add endpoint tests
Tests:
- returns unsupported / no forecast for ineligible events
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

Also add the initial auto-build trigger:
- when the main draw becomes full/ready after qualifying is complete, build the forecast automatically

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
