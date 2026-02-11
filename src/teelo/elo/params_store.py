"""Persistence helpers for active ELO parameter sets."""

from __future__ import annotations

from dataclasses import asdict

from sqlalchemy.orm import Session

from teelo.db.models import EloParameterSet
from teelo.elo.pipeline import EloParams

DEFAULT_PARAMS_VERSION = "defaults-v1"


def get_active_elo_params(session: Session) -> tuple[EloParams, str]:
    """Return active persisted ELO params, or defaults if none are active."""
    active = (
        session.query(EloParameterSet)
        .filter(EloParameterSet.is_active.is_(True))
        .order_by(EloParameterSet.created_at.desc())
        .first()
    )
    if not active:
        return EloParams(), DEFAULT_PARAMS_VERSION

    try:
        params = EloParams(**active.params)
    except Exception:
        return EloParams(), DEFAULT_PARAMS_VERSION

    return params, active.name


def persist_elo_params(
    session: Session,
    name: str,
    params: EloParams,
    source: str = "manual",
    activate: bool = False,
) -> EloParameterSet:
    """Persist a named ELO params set and optionally activate it."""
    if activate:
        session.query(EloParameterSet).update({EloParameterSet.is_active: False})

    record = EloParameterSet(
        name=name,
        params=asdict(params),
        source=source,
        is_active=activate,
    )
    session.add(record)
    session.flush()
    return record
