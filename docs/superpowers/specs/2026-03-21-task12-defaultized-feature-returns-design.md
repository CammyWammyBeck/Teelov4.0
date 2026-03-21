# Task 12 Defaultized Feature Returns Design

## Goal
Align feature outputs to never return `None` for insufficient sample sizes or zero denominators, while preserving `None` only for missing metadata (for example missing surface/date).

## Scope
Update only the six feature-group modules listed by Task 12:
- `form.py`
- `elo.py`
- `h2h.py`
- `activity.py`
- `opponent_quality.py`
- `tournament_history.py`

## Behavioral Rules
- Rates/proportions with zero denominator return `0.5`.
- Diffs/averages/counts with insufficient history return `0.0`.
- Ratios with non-positive denominator return `1.0`.
- Metadata-missing cases (such as `surface is None` or `match_date is None`) remain `None`.

## Test Impact
Existing unit tests that assert `None` for insufficient sample size or empty history will be updated to assert the new defaults.
