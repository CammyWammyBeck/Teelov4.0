"""Admin authentication helpers for the web admin area."""

from __future__ import annotations

import hashlib
import hmac
import os
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from teelo.db.models import AdminUser

PBKDF2_ALGORITHM = "sha256"
PBKDF2_ITERATIONS = 390_000
SALT_SIZE = 16


def hash_password(password: str) -> str:
    """Hash a plaintext password using PBKDF2-HMAC-SHA256."""
    if not password:
        raise ValueError("Password cannot be empty")

    salt = os.urandom(SALT_SIZE).hex()
    derived = hashlib.pbkdf2_hmac(
        PBKDF2_ALGORITHM,
        password.encode("utf-8"),
        bytes.fromhex(salt),
        PBKDF2_ITERATIONS,
    ).hex()
    return f"pbkdf2_{PBKDF2_ALGORITHM}${PBKDF2_ITERATIONS}${salt}${derived}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a plaintext password against a stored PBKDF2 hash."""
    try:
        scheme, iter_raw, salt_hex, expected_hex = stored_hash.split("$", 3)
        if scheme != f"pbkdf2_{PBKDF2_ALGORITHM}":
            return False
        iterations = int(iter_raw)
        actual_hex = hashlib.pbkdf2_hmac(
            PBKDF2_ALGORITHM,
            password.encode("utf-8"),
            bytes.fromhex(salt_hex),
            iterations,
        ).hex()
        return hmac.compare_digest(actual_hex, expected_hex)
    except Exception:
        return False


def authenticate_admin(
    db: Session,
    username: str,
    password: str,
) -> Optional[AdminUser]:
    """Return active admin user when credentials are valid."""
    admin = db.query(AdminUser).filter(AdminUser.username == username).first()
    if not admin or not admin.is_active:
        return None
    if not verify_password(password, admin.password_hash):
        return None
    return admin


def create_or_update_admin_user(
    db: Session,
    username: str,
    password: str,
    is_active: bool = True,
) -> AdminUser:
    """Create a new admin user, or update an existing one by username."""
    normalized = username.strip().lower()
    if not normalized:
        raise ValueError("Username cannot be empty")

    admin = db.query(AdminUser).filter(AdminUser.username == normalized).first()
    password_hash = hash_password(password)
    if admin:
        admin.password_hash = password_hash
        admin.is_active = is_active
    else:
        admin = AdminUser(
            username=normalized,
            password_hash=password_hash,
            is_active=is_active,
        )
        db.add(admin)
    db.flush()
    return admin


def mark_admin_login(db: Session, admin: AdminUser) -> None:
    """Record login timestamp for auditability."""
    admin.last_login_at = datetime.utcnow()
    db.flush()
