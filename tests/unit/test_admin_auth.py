"""Unit tests for admin authentication helpers."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from teelo.db.models import AdminUser
from teelo.web.admin_auth import (
    authenticate_admin,
    create_or_update_admin_user,
    hash_password,
    verify_password,
)


def _session():
    engine = create_engine("sqlite:///:memory:")
    AdminUser.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_hash_and_verify_password():
    hashed = hash_password("secret123")
    assert hashed.startswith("pbkdf2_sha256$")
    assert verify_password("secret123", hashed)
    assert not verify_password("wrong", hashed)


def test_create_or_update_admin_user():
    session = _session()
    try:
        created = create_or_update_admin_user(session, "Admin", "onepass")
        session.commit()
        assert created.username == "admin"

        updated = create_or_update_admin_user(session, "admin", "twopass", is_active=False)
        session.commit()
        assert updated.id == created.id
        assert not updated.is_active
        assert verify_password("twopass", updated.password_hash)
    finally:
        session.close()


def test_authenticate_admin_success_and_failure():
    session = _session()
    try:
        create_or_update_admin_user(session, "cammy", "strongpass")
        session.commit()

        ok = authenticate_admin(session, "cammy", "strongpass")
        assert ok is not None
        assert ok.username == "cammy"

        wrong_pw = authenticate_admin(session, "cammy", "wrong")
        assert wrong_pw is None

        user = session.query(AdminUser).filter(AdminUser.username == "cammy").first()
        user.is_active = False
        session.commit()

        inactive = authenticate_admin(session, "cammy", "strongpass")
        assert inactive is None
    finally:
        session.close()
