#!/usr/bin/env python3
"""Create or update a Teelo admin user."""

from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.session import get_session
from teelo.web.admin_auth import create_or_update_admin_user


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or update an admin user")
    parser.add_argument("--username", required=True, help="Admin username")
    parser.add_argument(
        "--password",
        default=None,
        help="Admin password (omit to be prompted securely)",
    )
    parser.add_argument(
        "--inactive",
        action="store_true",
        help="Create/update user as inactive",
    )
    args = parser.parse_args()

    password = args.password
    if password is None:
        password = getpass.getpass("Password: ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Passwords do not match.", file=sys.stderr)
            return 1

    with get_session() as session:
        admin = create_or_update_admin_user(
            db=session,
            username=args.username,
            password=password,
            is_active=not args.inactive,
        )
        print(
            f"Admin user ready: id={admin.id}, username={admin.username}, active={admin.is_active}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
