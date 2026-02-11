"""
ELO rating system module.

Implements tennis-specific ELO calculations with:
- Tournament level-specific K and S factors
- Margin-of-victory K-factor scaling
- Inactivity decay (pulls inactive players toward 1500)
- New/returning player K-factor boost
- Bayesian parameter optimization via Optuna
"""

from teelo.elo.calculator import EloCalculator, EloUpdate, calculate_fast
from teelo.elo.constants import ELO_CONSTANTS
from teelo.elo.decay import apply_inactivity_decay
from teelo.elo.boost import calculate_k_boost
from teelo.elo.pipeline import EloPipeline, EloParams, load_matches_for_elo
from teelo.elo.live import LiveEloUpdater
from teelo.elo.params_store import get_active_elo_params, persist_elo_params

__all__ = [
    "EloCalculator",
    "EloUpdate",
    "ELO_CONSTANTS",
    "calculate_fast",
    "apply_inactivity_decay",
    "calculate_k_boost",
    "EloPipeline",
    "EloParams",
    "load_matches_for_elo",
    "LiveEloUpdater",
    "get_active_elo_params",
    "persist_elo_params",
]
