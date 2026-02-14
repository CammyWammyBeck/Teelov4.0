"""Database advisory lock helpers for single-run orchestration safety."""

from __future__ import annotations

import hashlib
import time
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import text
from sqlalchemy.engine import Engine


def advisory_lock_key(name: str) -> int:
    """Return a deterministic signed 64-bit lock key from a stage/pipeline name."""
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


@contextmanager
def postgres_advisory_lock(
    engine: Engine,
    *,
    key: int,
    timeout_seconds: float = 0.0,
    poll_interval_seconds: float = 1.0,
) -> Generator[bool, None, None]:
    """
    Acquire a PostgreSQL advisory lock for the life of this context.

    Yields:
        True if lock acquired.

    Raises:
        TimeoutError: if lock cannot be acquired before timeout.
    """
    connection = engine.connect()
    acquired = False
    try:
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        while True:
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:key)"),
                    {"key": key},
                ).scalar()
            )
            if acquired:
                break
            if timeout_seconds <= 0:
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(max(poll_interval_seconds, 0.05))

        if not acquired:
            raise TimeoutError(f"Could not acquire advisory lock key={key}")

        yield True
    finally:
        if acquired:
            connection.execute(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": key},
            )
        connection.close()
