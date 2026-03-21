# Teelo v4.0 — Complete Feature Reference

**Last updated**: 2026-03-21
**Total features**: 158 (current) → ~235 after Phase 1

This document lists every feature in the ML pipeline, how it's calculated, and when it returns `None`. Keep this up to date when feature groups are added, modified, or removed.

---

## Feature Groups Overview

| Group | File | Features | Status |
|---|---|---|---|
| context | `groups/context.py` | 22 (+3 planned) | Active |
| elo_core | `groups/elo.py` | 12 (+4 planned) | Active |
| elo_history | `groups/elo.py` | 2 | Active |
| elo_variance | `groups/elo.py` | 8 | Active |
| form | `groups/form.py` | 38 | Active |
| h2h | `groups/h2h.py` | 12 | Active |
| activity | `groups/activity.py` | 18 | Active |
| opponent_quality | `groups/opponent_quality.py` | 10 | Active |
| dominance | `groups/dominance.py` | 14 | Active (will be absorbed into score_profile) |
| fatigue | `groups/fatigue.py` | 7 | Active |
| tournament_history | `groups/tournament_history.py` | 4 | Active |
| confidence | `groups/confidence.py` | 13 | Active |
| score_profile | `groups/score_profile.py` | 56 | Planned (Phase 1) — replaces dominance |
| country_performance | `groups/country_performance.py` | 14 | Planned (Phase 1) |

---

## 1. Context Features (`context.py`) — 22 features

One-hot encodings for surface, level, round, and tour metadata.

| Feature | Calculation | None When |
|---|---|---|
| `surface_hard` | 1.0 if surface is Hard | Surface missing |
| `surface_clay` | 1.0 if surface is Clay | Surface missing |
| `surface_grass` | 1.0 if surface is Grass | Surface missing |
| `surface_indoor` | 1.0 if surface is Indoor (currently always 0) | Surface missing |
| `level_G` | 1.0 if Grand Slam | Never |
| `level_M` | 1.0 if Masters | Never |
| `level_A` | 1.0 if ATP/WTA 250/500 | Never |
| `level_C` | 1.0 if Challenger | Never |
| `level_F` | 1.0 if Futures/ITF | Never |
| `round_F` | 1.0 if Final | Round missing |
| `round_SF` | 1.0 if Semi-final | Round missing |
| `round_QF` | 1.0 if Quarter-final | Round missing |
| `round_R16` | 1.0 if Round of 16 | Round missing |
| `round_R32` | 1.0 if Round of 32 | Round missing |
| `round_R64` | 1.0 if Round of 64 | Round missing |
| `round_R128` | 1.0 if Round of 128 | Round missing |
| `round_Q1` | 1.0 if Qualifying R1 | Round missing |
| `round_Q2` | 1.0 if Qualifying R2 | Round missing |
| `round_Q3` | 1.0 if Qualifying R3 | Round missing |
| `round_RR` | 1.0 if Round Robin | Round missing |
| `tour_wta` | 1.0 if WTA tour | Tour missing |
| `year` | Float year (e.g. 2026.0) | Year missing |

### Planned additions (Phase 1):

| Feature | Calculation | None When |
|---|---|---|
| `month_sin` | `sin(2π × month / 12)` | Match date missing |
| `month_cos` | `cos(2π × month / 12)` | Match date missing |
| `year_progress` | `day_of_year / 365` | Match date missing |

---

## 2. ELO Features (`elo.py`) — 22 features (3 sub-groups)

### 2a. ELO Core — 12 features

| Feature | Calculation | None When |
|---|---|---|
| `elo_a` | Player A current ELO | Never (default 1500) |
| `elo_b` | Player B current ELO | Never |
| `elo_diff` | `elo_a - elo_b` | Never |
| `surface_elo_a` | Player A surface-specific ELO | Surface missing |
| `surface_elo_b` | Player B surface-specific ELO | Surface missing |
| `surface_elo_diff` | `surface_elo_a - surface_elo_b` | Either surface ELO None |
| `peak_elo_a` | Player A peak career ELO | Never |
| `peak_elo_b` | Player B peak career ELO | Never |
| `peak_ratio_a` | `elo_current / elo_peak` | `elo_peak <= 0` |
| `peak_ratio_b` | Same for player B | `elo_peak <= 0` |
| `elo_expected_a` | Expected win prob for A from ELO diff | Never |
| `elo_expected_b` | `1 - elo_expected_a` | Never |

### Planned additions to ELO Core (Phase 1):

| Feature | Calculation | None When |
|---|---|---|
| `surface_gap_a` | `surface_elo[surface] - elo_current` | Surface missing or no surface ELO |
| `surface_gap_b` | Same for player B | Same |
| `off_surface_elo_a` | Average ELO across other surfaces | Surface missing or < 1 other surface |
| `off_surface_elo_b` | Same for player B | Same |

### 2b. ELO History — 2 features

| Feature | Calculation | Min Sample | None When |
|---|---|---|---|
| `elo_momentum_a` | `last_elo - first_elo` in history window (up to 8) | 2 entries | History < 2 |
| `elo_momentum_b` | Same for player B | 2 entries | Same |

### 2c. ELO Variance — 8 features

| Feature | Calculation | Min Sample | None When |
|---|---|---|---|
| `elo_var_8_a` | Sample variance of ELO deltas, last 8 | 3 entries | Window < 3 |
| `elo_var_8_b` | Same for player B | 3 entries | Same |
| `elo_var_16_a` | Same, last 16 | 3 entries | Same |
| `elo_var_16_b` | Same for player B | 3 entries | Same |
| `elo_var_32_a` | Same, last 32 | 3 entries | Same |
| `elo_var_32_b` | Same for player B | 3 entries | Same |
| `elo_var_64_a` | Same, last 64 | 3 entries | Same |
| `elo_var_64_b` | Same for player B | 3 entries | Same |

---

## 3. Form Features (`form.py`) — 38 features

Rolling win rates across time windows plus surface/level/career rates.

### Win rate by time window (16 features)

| Feature | Window | Min Sample | None When |
|---|---|---|---|
| `win_rate_4w_a`, `win_rate_4w_b` | 28 days | 5 matches | Date missing or < 5 matches |
| `win_rate_8w_a`, `win_rate_8w_b` | 56 days | 5 matches | Same |
| `win_rate_16w_a`, `win_rate_16w_b` | 112 days | 5 matches | Same |
| `win_rate_32w_a`, `win_rate_32w_b` | 224 days | 5 matches | Same |
| `win_rate_64w_a`, `win_rate_64w_b` | 448 days | 5 matches | Same |
| `win_rate_128w_a`, `win_rate_128w_b` | 896 days | 5 matches | Same |
| `win_rate_256w_a`, `win_rate_256w_b` | 1792 days | 5 matches | Same |
| `win_rate_512w_a`, `win_rate_512w_b` | 3584 days | 5 matches | Same |

### Match count by time window (16 features)

| Feature | Window | None When |
|---|---|---|
| `match_count_4w_a`, `match_count_4w_b` | 28 days | Date missing |
| `match_count_8w_a`, `match_count_8w_b` | 56 days | Same |
| `match_count_16w_a`, `match_count_16w_b` | 112 days | Same |
| `match_count_32w_a`, `match_count_32w_b` | 224 days | Same |
| `match_count_64w_a`, `match_count_64w_b` | 448 days | Same |
| `match_count_128w_a`, `match_count_128w_b` | 896 days | Same |
| `match_count_256w_a`, `match_count_256w_b` | 1792 days | Same |
| `match_count_512w_a`, `match_count_512w_b` | 3584 days | Same |

### Static rates (6 features)

| Feature | Calculation | Min Sample | None When |
|---|---|---|---|
| `surface_win_rate_a` | `surface_wins / (surface_wins + surface_losses)` | 5 surface matches | Surface missing or < 5 |
| `surface_win_rate_b` | Same for player B | 5 surface matches | Same |
| `level_win_rate_a` | `level_wins / (level_wins + level_losses)` | 5 level matches | < 5 at level |
| `level_win_rate_b` | Same for player B | 5 level matches | Same |
| `career_win_rate_a` | `wins_total / (wins_total + losses_total)` | 10 career matches | < 10 career |
| `career_win_rate_b` | Same for player B | 10 career matches | Same |

---

## 4. Head-to-Head Features (`h2h.py`) — 12 features

| Feature | Calculation | None When |
|---|---|---|
| `h2h_a_wins` | All-time H2H wins for player A | Never (0 if no history) |
| `h2h_b_wins` | All-time H2H wins for player B | Never |
| `h2h_a_wins_2y` | H2H wins in last 730 days | Match date missing |
| `h2h_b_wins_2y` | Same for player B | Same |
| `h2h_a_wins_6m` | H2H wins in last 182 days | Match date missing |
| `h2h_b_wins_6m` | Same for player B | Same |
| `h2h_a_wins_surface` | H2H wins on current surface | Surface missing |
| `h2h_b_wins_surface` | Same for player B | Same |
| `h2h_a_wins_level` | H2H wins at current level | Never |
| `h2h_b_wins_level` | Same for player B | Never |
| `h2h_total` | `h2h_a_wins + h2h_b_wins` | Never |
| `h2h_a_dominance` | `h2h_a_wins / h2h_total` | `h2h_total == 0` |

---

## 5. Activity Features (`activity.py`) — 18 features

| Feature | Calculation | None When |
|---|---|---|
| `days_since_last_a` | Days since player A's last match | Either date missing |
| `days_since_last_b` | Same for player B | Same |
| `days_since_debut_a` | Days since player A's first match | Either date missing |
| `days_since_debut_b` | Same for player B | Same |
| `matches_4w_a` | Matches in last 28 days | Match date missing |
| `matches_4w_b` | Same for player B | Same |
| `matches_8w_a` | Matches in last 56 days | Same |
| `matches_8w_b` | Same for player B | Same |
| `matches_16w_a` | Matches in last 112 days | Same |
| `matches_16w_b` | Same for player B | Same |
| `games_last_match_a` | `games_won + games_lost` from most recent match | No prior matches |
| `games_last_match_b` | Same for player B | Same |
| `games_tournament_a` | Total games in current tournament edition | Never (0 fallback) |
| `games_tournament_b` | Same for player B | Never |
| `seed_a` | Player A seed as float | Seed missing or 0 |
| `seed_b` | Player B seed as float | Seed missing or 0 |
| `seed_diff` | `seed_a - seed_b` | Either seed None |
| `both_seeded` | 1.0 if both seeded, else 0.0 | Never |

---

## 6. Opponent Quality Features (`opponent_quality.py`) — 10 features

All computed from the last 8 matches.

| Feature | Calculation | None When |
|---|---|---|
| `opp_elo_avg_8_a` | Average opponent ELO (last 8, non-None) | No usable values |
| `opp_elo_avg_8_b` | Same for player B | Same |
| `opp_surface_elo_avg_8_a` | Average opponent surface ELO (last 8) | No usable values |
| `opp_surface_elo_avg_8_b` | Same for player B | Same |
| `wins_vs_higher_elo_8_a` | Count of wins where `expected_win_prob < 0.5` | No recent matches |
| `wins_vs_higher_elo_8_b` | Same for player B | Same |
| `losses_vs_lower_elo_8_a` | Count of losses where `expected_win_prob > 0.5` | No recent matches |
| `losses_vs_lower_elo_8_b` | Same for player B | Same |
| `elo_overperf_8_a` | Mean `(actual - expected_win_prob)` over last 8 | No usable residuals |
| `elo_overperf_8_b` | Same for player B | Same |

---

## 7. Dominance Features (`dominance.py`) — 14 features

**Status**: Will be absorbed into score_profile in Phase 1.

All computed from last 8 matches. Min sample: 3.

| Feature | Calculation | None When |
|---|---|---|
| `game_diff_avg_8_a` | Mean `(games_won - games_lost)` | No matches |
| `game_diff_avg_8_b` | Same for player B | Same |
| `set_diff_avg_8_a` | Mean `(sets_won - sets_lost)` | No matches |
| `set_diff_avg_8_b` | Same for player B | Same |
| `straight_sets_rate_8_a` | Proportion with `straight_sets=True` | < 3 matches |
| `straight_sets_rate_8_b` | Same for player B | Same |
| `deciding_set_rate_8_a` | Proportion with `deciding_set_played=True` | < 3 matches |
| `deciding_set_rate_8_b` | Same for player B | Same |
| `tiebreak_rate_8_a` | Proportion with `tiebreaks_played > 0` | < 3 matches |
| `tiebreak_rate_8_b` | Same for player B | Same |
| `tiebreak_win_rate_8_a` | `tiebreaks_won / tiebreaks_played` | < 3 matches OR no tiebreaks |
| `tiebreak_win_rate_8_b` | Same for player B | Same |
| `close_match_rate_8_a` | Proportion with `close_match=True` | < 3 matches |
| `close_match_rate_8_b` | Same for player B | Same |

---

## 8. Fatigue Features (`fatigue.py`) — 7 features

| Feature | Calculation | None When |
|---|---|---|
| `rest_days_a` | Days since player A's last match | Either date missing |
| `rest_days_b` | Same for player B | Same |
| `rest_days_diff_ab` | `rest_days_a - rest_days_b` | Either rest day None |
| `matches_7d_diff_ab` | Match count diff in last 7 days | Never (0 fallback) |
| `games_7d_diff_ab` | Games sum diff in last 7 days | Never |
| `matches_this_tournament_diff_ab` | Tournament match count diff | Never (0 fallback) |
| `games_this_tournament_diff_ab` | Tournament games sum diff | Never |

---

## 9. Tournament History Features (`tournament_history.py`) — 4 features

| Feature | Calculation | Min Sample | None When |
|---|---|---|---|
| `tournament_match_count_a` | Matches at this tournament (all editions) | — | Never (0 fallback) |
| `tournament_match_count_b` | Same for player B | — | Never |
| `tournament_win_rate_a` | `tournament_wins / tournament_matches` | 2 matches | Tournament ID missing or < 2 |
| `tournament_win_rate_b` | Same for player B | 2 matches | Same |

---

## 10. Confidence Features (`confidence.py`) — 13 features

Meta-features indicating data availability and sample sizes.

| Feature | Calculation | None When |
|---|---|---|
| `surface_elo_observed_a` | 1.0 if player has observed surface ELO | Never |
| `surface_elo_observed_b` | Same for player B | Never |
| `surface_elo_default_a` | 1.0 if surface ELO is default (not observed) | Never |
| `surface_elo_default_b` | Same for player B | Never |
| `surface_elo_match_count_a` | Matches on this surface | Never (0 fallback) |
| `surface_elo_match_count_b` | Same for player B | Never |
| `opponent_quality_sample_count_8_a` | Count of last-8 with non-None expected_win_prob | Never |
| `opponent_quality_sample_count_8_b` | Same for player B | Never |
| `tournament_history_sample_count_a` | Matches at this tournament | Never (0 fallback) |
| `tournament_history_sample_count_b` | Same for player B | Never |
| `h2h_sample_count` | Total H2H records vs opponent | Never |
| `h2h_surface_sample_count` | H2H records on this surface | Never |
| `match_date_estimated_flag` | 1.0 if match date is estimated | Never |

---

## Planned: Score Profile Features (`score_profile.py`) — 56 features

**Phase 1** — Replaces dominance group. Two windows (8 and 64 matches) plus clutch matchup (128 matches).

### Score Profile — Window 8 (min sample 3)

| Feature | Calculation | None When |
|---|---|---|
| `game_diff_avg_8_a`, `_b` | Mean `(games_won - games_lost)` | No matches |
| `set_diff_avg_8_a`, `_b` | Mean `(sets_won - sets_lost)` | No matches |
| `straight_sets_rate_8_a`, `_b` | Proportion with `straight_sets=True` | < 3 matches |
| `deciding_set_rate_8_a`, `_b` | Proportion with `deciding_set_played=True` | < 3 matches |
| `tiebreak_rate_8_a`, `_b` | Proportion with `tiebreaks_played > 0` | < 3 matches |
| `tiebreak_win_rate_8_a`, `_b` | `tiebreaks_won / tiebreaks_played` | < 3 matches OR no tiebreaks |
| `tiebreaks_played_8_a`, `_b` | Count of tiebreaks played in window | Never (0 fallback) |
| `close_match_rate_8_a`, `_b` | Proportion with `close_match=True` | < 3 matches |

### Score Profile — Window 64 (min sample 5)

| Feature | Calculation | None When |
|---|---|---|
| `game_diff_avg_64_a`, `_b` | Mean `(games_won - games_lost)` | No matches |
| `set_diff_avg_64_a`, `_b` | Mean `(sets_won - sets_lost)` | No matches |
| `straight_sets_rate_64_a`, `_b` | Proportion with `straight_sets=True` | < 5 matches |
| `deciding_set_rate_64_a`, `_b` | Proportion with `deciding_set_played=True` | < 5 matches |
| `tiebreak_rate_64_a`, `_b` | Proportion with `tiebreaks_played > 0` | < 5 matches |
| `tiebreak_win_rate_64_a`, `_b` | `tiebreaks_won / tiebreaks_played` | < 5 matches OR no tiebreaks |
| `tiebreaks_played_64_a`, `_b` | Count of tiebreaks played in window | Never (0 fallback) |
| `close_match_rate_64_a`, `_b` | Proportion with `close_match=True` | < 5 matches |
| `deciding_set_win_rate_64_a`, `_b` | Wins in deciding sets / deciding sets played | < 5 matches OR no deciding sets |
| `deciding_sets_played_64_a`, `_b` | Count of deciding sets in window | Never (0 fallback) |
| `comeback_rate_64_a`, `_b` | Wins after losing 1st set / first sets lost | < 5 matches OR no first sets lost |
| `first_sets_lost_64_a`, `_b` | Count of first sets lost in window | Never (0 fallback) |
| `straight_sets_win_rate_64_a`, `_b` | `straight_sets AND won` / total wins | < 5 matches OR no wins |

### Clutch Matchup — Window 128 (min sample 5)

Clutch score = `0.4 * tiebreak_win_rate + 0.3 * deciding_set_win_rate + 0.3 * comeback_rate` (from 64-match window). Thresholds: clutch > 0.55, non-clutch < 0.40.

| Feature | Calculation | None When |
|---|---|---|
| `vs_clutch_win_rate_a`, `_b` | Win rate vs clutch opponents (last 128) | < 5 clutch matches |
| `vs_clutch_matches_a`, `_b` | Count of clutch opponent matches | Never (0 fallback) |
| `vs_normal_clutch_win_rate_a`, `_b` | Win rate vs normal opponents | < 5 normal matches |
| `vs_normal_clutch_matches_a`, `_b` | Count of normal opponent matches | Never (0 fallback) |
| `vs_non_clutch_win_rate_a`, `_b` | Win rate vs non-clutch opponents | < 5 non-clutch matches |
| `vs_non_clutch_matches_a`, `_b` | Count of non-clutch opponent matches | Never (0 fallback) |
| `opponent_clutch_score_a`, `_b` | Opponent's raw clutch_score | Opponent has no clutch score |

---

## Planned: Country Performance Features (`country_performance.py`) — 14 features

**Phase 1** — All-time records (not windowed). Min sample: 5 matches for rates.

| Feature | Calculation | None When |
|---|---|---|
| `country_win_rate_a`, `_b` | Win rate in tournament country | < 5 country matches or country unknown |
| `country_matches_a`, `_b` | Matches played in this country | Never (0 fallback) |
| `country_delta_a`, `_b` | `country_win_rate - career_win_rate` | Either rate is None |
| `region_win_rate_a`, `_b` | Win rate in this region | < 5 region matches or region unknown |
| `region_matches_a`, `_b` | Matches played in this region | Never (0 fallback) |
| `region_delta_a`, `_b` | `region_win_rate - career_win_rate` | Either rate is None |
| `is_home_a`, `_b` | 1.0 if nationality == tournament country | Nationality or country unknown |

---

## Feature Presets

| Preset | Description | Feature Count |
|---|---|---|
| `full` | context + elo (all 3) + form + h2h + activity | 110 |
| `trimmed` | `full` minus 20 named features | 90 |
| `baseline_v2` | `full` + opponent_quality + dominance + fatigue + tournament_history + confidence | 158 |
| `trimmed_v2` | `baseline_v2` minus 63 named features | 95 |
| `trimmed_v2b` | `baseline_v2` minus 73 named features (current production) | 85 |

After Phase 1, the `baseline_v2` equivalent will include score_profile (replacing dominance) + country_performance + calendar + surface gap extensions = ~235 features.
