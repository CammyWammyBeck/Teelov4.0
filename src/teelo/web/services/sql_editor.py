"""Service layer for the admin SQL editor.

Handles query classification, execution, preview for mutations,
and audit logging.
"""

import re
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from teelo.db.models import AdminQueryLog

# Query timeout in seconds
QUERY_TIMEOUT_MS = 30_000

# DDL patterns to block
_DDL_PATTERN = re.compile(
    r"^\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Mutation patterns
_MUTATION_PATTERN = re.compile(
    r"^\s*(UPDATE|DELETE|INSERT)\b",
    re.IGNORECASE,
)

_SELECT_PATTERN = re.compile(
    r"^\s*(SELECT|WITH)\b",
    re.IGNORECASE,
)


_TABLE_FROM_PATTERN = re.compile(
    r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b",
    re.IGNORECASE,
)


def parse_table_name(sql: str) -> Optional[str]:
    """Extract the primary table name from a SELECT query."""
    m = _TABLE_FROM_PATTERN.search(sql)
    return m.group(1) if m else None


def get_pk_columns(db: Session, table_name: str) -> list[str]:
    """Return primary key column names for a table."""
    pk_sql = text("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = :table
        ORDER BY kcu.ordinal_position
    """)
    return [row[0] for row in db.execute(pk_sql, {"table": table_name}).fetchall()]


def classify_query(sql: str) -> str:
    """Return query type: 'ddl', 'select', 'update', 'delete', 'insert', or 'unknown'."""
    stripped = sql.strip().rstrip(";").strip()
    if _DDL_PATTERN.match(stripped):
        return "ddl"
    if _SELECT_PATTERN.match(stripped):
        return "select"
    m = _MUTATION_PATTERN.match(stripped)
    if m:
        return m.group(1).lower()
    return "unknown"


def execute_select(db: Session, sql: str, page: int = 1, page_size: int = 50) -> dict:
    """Execute a SELECT query and return paginated results."""
    db.execute(text(f"SET LOCAL statement_timeout = '{QUERY_TIMEOUT_MS}ms'"))

    result = db.execute(text(sql))
    columns = list(result.keys())
    all_rows = [list(row) for row in result.fetchall()]
    total = len(all_rows)

    start = (page - 1) * page_size
    end = start + page_size
    page_rows = all_rows[start:end]

    # Convert non-serializable types to strings
    for row in page_rows:
        for i, val in enumerate(row):
            if val is not None and not isinstance(val, (str, int, float, bool)):
                row[i] = str(val)

    # Include table/PK metadata for inline editing
    table_name = parse_table_name(sql)
    pk_columns = get_pk_columns(db, table_name) if table_name else []

    return {
        "columns": columns,
        "rows": page_rows,
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
        "table_name": table_name,
        "pk_columns": pk_columns,
    }


def preview_mutation(db: Session, sql: str) -> dict:
    """Preview a mutation by running it in a transaction and returning affected count.

    Uses a SAVEPOINT so the outer transaction is not committed.
    """
    db.execute(text(f"SET LOCAL statement_timeout = '{QUERY_TIMEOUT_MS}ms'"))

    # Create savepoint
    db.execute(text("SAVEPOINT mutation_preview"))

    try:
        result = db.execute(text(sql))
        affected = result.rowcount

        return {
            "affected_rows": affected,
        }
    finally:
        # Always rollback the savepoint
        db.execute(text("ROLLBACK TO SAVEPOINT mutation_preview"))


def execute_mutation(db: Session, sql: str) -> dict:
    """Execute a mutation query and commit."""
    db.execute(text(f"SET LOCAL statement_timeout = '{QUERY_TIMEOUT_MS}ms'"))
    result = db.execute(text(sql))
    affected = result.rowcount
    db.commit()
    return {"affected_rows": affected}


def log_query(
    db: Session,
    admin_user_id: int,
    query_text: str,
    query_type: str,
    affected_rows: Optional[int],
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    """Write an entry to the admin query audit log."""
    entry = AdminQueryLog(
        admin_user_id=admin_user_id,
        query_text=query_text,
        query_type=query_type,
        affected_rows=affected_rows,
        success=success,
        error_message=error_message,
        executed_at=datetime.utcnow(),
    )
    db.add(entry)
    db.commit()


def get_schema_info(db: Session) -> list[dict]:
    """Return all user tables and their columns from information_schema."""
    tables_sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tables = [row[0] for row in db.execute(tables_sql).fetchall()]

    columns_sql = text("""
        SELECT table_name, column_name, data_type, is_nullable,
               column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """)
    columns_by_table = {}
    for row in db.execute(columns_sql).fetchall():
        tname = row[0]
        if tname not in columns_by_table:
            columns_by_table[tname] = []
        columns_by_table[tname].append({
            "name": row[1],
            "type": row[2],
            "nullable": row[3] == "YES",
            "default": row[4],
        })

    # Get primary keys
    pk_sql = text("""
        SELECT kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
    """)
    pk_set = set()
    for row in db.execute(pk_sql).fetchall():
        pk_set.add((row[0], row[1]))

    result = []
    for table in tables:
        cols = columns_by_table.get(table, [])
        for col in cols:
            col["primary_key"] = (table, col["name"]) in pk_set
        result.append({"table": table, "columns": cols})

    return result
