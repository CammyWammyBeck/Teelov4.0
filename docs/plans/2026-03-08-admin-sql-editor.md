# Admin SQL Editor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an in-browser SQL editor with schema browser, interactive results table, mutation preview/confirm, and audit logging to the admin section.

**Architecture:** New admin page at `/admin/sql` with three panels: schema sidebar, CodeMirror editor, results table. Backend endpoints handle query execution (with transaction-wrapped preview for mutations) and schema introspection. New `AdminQueryLog` model for audit trail.

**Tech Stack:** Python/FastAPI, SQLAlchemy raw SQL via `text()`, CodeMirror 6 (CDN), vanilla JS, Jinja2, Tailwind CSS.

---

### Task 1: Add `{% block scripts %}` to base.html

**Files:**
- Modify: `src/teelo/web/templates/base.html`

**Step 1: Add the block**

Add `{% block head_extra %}{% endblock %}` before `</head>` (line 35, before the lucide script) and `{% block scripts %}{% endblock %}` before `</body>` (line 238).

In `base.html`, add before line 238 (`</body>`):
```html
    {% block scripts %}{% endblock %}
```

And add before line 36 (`</head>`):
```html
    {% block head_extra %}{% endblock %}
```

**Step 2: Verify no existing templates break**

Run: `grep -r "block scripts\|block head_extra" src/teelo/web/templates/`
Expected: Only base.html matches.

**Step 3: Commit**

```bash
git add src/teelo/web/templates/base.html
git commit -m "feat: add head_extra and scripts blocks to base.html"
```

---

### Task 2: Create `AdminQueryLog` model

**Files:**
- Modify: `src/teelo/db/models.py`

**Step 1: Add the model**

Add after the `AdminUser` class in `models.py`:

```python
class AdminQueryLog(Base):
    """Audit log for SQL queries executed via the admin SQL editor."""

    __tablename__ = "admin_query_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    admin_user_id: Mapped[int] = mapped_column(
        ForeignKey("admin_users.id", ondelete="CASCADE")
    )
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    query_type: Mapped[str] = mapped_column(String(20), nullable=False)  # select, update, delete, insert
    affected_rows: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    admin_user: Mapped["AdminUser"] = relationship()

    __table_args__ = (
        Index("idx_admin_query_log_user", "admin_user_id"),
        Index("idx_admin_query_log_executed", "executed_at"),
    )
```

**Step 2: Generate migration**

```bash
cd /home/cammybeck/Documents/programming/Teelov4.0
source venv/bin/activate
alembic revision --autogenerate -m "add admin_query_log table"
```

**Step 3: Apply migration**

```bash
alembic upgrade head
```

**Step 4: Commit**

```bash
git add src/teelo/db/models.py alembic/versions/
git commit -m "feat: add AdminQueryLog model and migration"
```

---

### Task 3: Create SQL execution service

**Files:**
- Create: `src/teelo/web/services/sql_editor.py`

**Step 1: Create the service module**

```python
"""Service layer for the admin SQL editor.

Handles query classification, execution, preview for mutations,
and audit logging.
"""

import re
import time
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

    return {
        "columns": columns,
        "rows": page_rows,
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
    }


def preview_mutation(db: Session, sql: str) -> dict:
    """Preview a mutation by running it in a transaction and returning affected count + sample.

    Uses a SAVEPOINT so the outer transaction is not committed.
    """
    db.execute(text(f"SET LOCAL statement_timeout = '{QUERY_TIMEOUT_MS}ms'"))

    # Create savepoint
    db.execute(text("SAVEPOINT mutation_preview"))

    try:
        result = db.execute(text(sql))
        affected = result.rowcount

        # Try to get sample of affected rows by parsing table name
        sample_rows = []
        sample_columns = []

        return {
            "affected_rows": affected,
            "sample_columns": sample_columns,
            "sample_rows": sample_rows,
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
```

**Step 2: Commit**

```bash
git add src/teelo/web/services/sql_editor.py
git commit -m "feat: add SQL editor service layer"
```

---

### Task 4: Create admin SQL route handlers

**Files:**
- Modify: `src/teelo/web/services/legacy_main_handlers.py` (add 3 handlers)
- Modify: `src/teelo/web/routers/admin.py` (register routes)

**Step 1: Add handlers to legacy_main_handlers.py**

Add these imports at the top:
```python
from teelo.web.services.sql_editor import (
    classify_query,
    execute_mutation,
    execute_select,
    get_schema_info,
    log_query,
    preview_mutation,
)
```

Add these handlers:

```python
async def admin_sql_editor(
    request: Request,
    db: Session = Depends(get_db),
):
    """Render the SQL editor page."""
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    admin = _current_admin_user(request, db)
    schema = get_schema_info(db)
    return templates.TemplateResponse(
        "admin_sql.html",
        {
            "request": request,
            "admin": admin,
            "schema": schema,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


async def admin_sql_execute(
    request: Request,
    db: Session = Depends(get_db),
):
    """Execute a SQL query and return JSON results."""
    redirect = _require_admin(request, db)
    if redirect:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    admin = _current_admin_user(request, db)
    body = await request.json()
    sql = body.get("query", "").strip()
    action = body.get("action", "execute")  # "execute" or "preview" or "confirm"
    page = body.get("page", 1)

    if not sql:
        return JSONResponse({"error": "Empty query"}, status_code=400)

    query_type = classify_query(sql)

    if query_type == "ddl":
        return JSONResponse(
            {"error": "DDL statements (CREATE, ALTER, DROP, etc.) are not allowed."},
            status_code=400,
        )

    if query_type == "unknown":
        return JSONResponse(
            {"error": "Unrecognized query type. Only SELECT, INSERT, UPDATE, DELETE are supported."},
            status_code=400,
        )

    try:
        if query_type == "select":
            result = execute_select(db, sql, page=page)
            return JSONResponse({"type": "select", **result})

        # Mutation query
        if action == "preview":
            result = preview_mutation(db, sql)
            return JSONResponse({"type": "preview", **result})

        if action == "confirm":
            result = execute_mutation(db, sql)
            log_query(
                db,
                admin_user_id=admin.id,
                query_text=sql,
                query_type=query_type,
                affected_rows=result["affected_rows"],
                success=True,
            )
            return JSONResponse({"type": "mutation", **result})

        # Default for mutations: preview first
        result = preview_mutation(db, sql)
        return JSONResponse({"type": "preview", **result})

    except Exception as e:
        error_msg = str(e)
        if query_type in ("update", "delete", "insert"):
            log_query(
                db,
                admin_user_id=admin.id,
                query_text=sql,
                query_type=query_type,
                affected_rows=None,
                success=False,
                error_message=error_msg,
            )
        return JSONResponse({"error": error_msg}, status_code=400)


async def admin_sql_schema(
    request: Request,
    db: Session = Depends(get_db),
):
    """Return database schema as JSON."""
    redirect = _require_admin(request, db)
    if redirect:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    schema = get_schema_info(db)
    return JSONResponse({"tables": schema})
```

Add `JSONResponse` import:
```python
from fastapi.responses import JSONResponse
```

**Step 2: Register routes in admin.py**

Add these lines to `src/teelo/web/routers/admin.py`:
```python
router.add_api_route('/admin/sql', legacy.admin_sql_editor, methods=['GET'], response_class=legacy.HTMLResponse)
router.add_api_route('/admin/sql/execute', legacy.admin_sql_execute, methods=['POST'])
router.add_api_route('/admin/sql/schema', legacy.admin_sql_schema, methods=['GET'])
```

**Step 3: Commit**

```bash
git add src/teelo/web/services/legacy_main_handlers.py src/teelo/web/routers/admin.py
git commit -m "feat: add SQL editor route handlers"
```

---

### Task 5: Create the SQL editor template

**Files:**
- Create: `src/teelo/web/templates/admin_sql.html`

**Step 1: Create the template**

```html
{% extends "base.html" %}

{% block title %}SQL Editor | Admin | Teelo{% endblock %}

{% block head_extra %}
<!-- CodeMirror 6 -->
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.18/codemirror.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.18/theme/dracula.min.css">
<style>
    .CodeMirror {
        height: 200px;
        border: 1px solid #e5e7eb;
        border-radius: 0.75rem;
        font-size: 14px;
    }
    .schema-col { max-height: calc(100vh - 12rem); overflow-y: auto; }
    .results-table-wrap { max-height: 400px; overflow: auto; }
    .results-table th { position: sticky; top: 0; z-index: 1; }
    .results-table td { cursor: pointer; }
    .results-table td:hover { background: #f0fdf4; }
    .toast {
        position: fixed; bottom: 1rem; right: 1rem; z-index: 100;
        padding: 0.5rem 1rem; border-radius: 0.5rem; font-size: 0.875rem;
        color: #fff; background: #16a34a; opacity: 0; transition: opacity 0.3s;
    }
    .toast.show { opacity: 1; }
</style>
{% endblock %}

{% block content %}
<section class="space-y-4">
    <!-- Header -->
    <div class="flex items-center justify-between gap-4">
        <div>
            <h1 class="text-3xl font-extrabold tracking-tight text-teelo-dark">SQL Editor</h1>
            <p class="text-sm text-gray-500 mt-1">
                <a href="/admin" class="hover:underline">Admin</a> &rarr; SQL Editor
            </p>
        </div>
        <form method="post" action="/admin/logout">
            <button type="submit"
                    class="rounded-lg border border-gray-300 px-4 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-50 transition">
                Log out
            </button>
        </form>
    </div>

    <div class="flex gap-4">
        <!-- Schema Sidebar -->
        <div class="w-64 flex-shrink-0 schema-col bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
            <h2 class="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Tables</h2>
            <div id="schema-browser">
                {% for table_info in schema %}
                <div class="mb-1">
                    <button class="schema-table-btn w-full text-left px-2 py-1.5 rounded-lg text-sm font-medium text-teelo-dark hover:bg-gray-100 transition flex items-center justify-between"
                            data-table="{{ table_info.table }}">
                        <span>{{ table_info.table }}</span>
                        <i data-lucide="chevron-right" class="w-4 h-4 text-gray-400 schema-chevron transition-transform"></i>
                    </button>
                    <div class="schema-columns hidden pl-4 pb-2">
                        {% for col in table_info.columns %}
                        <div class="flex items-center gap-2 py-0.5 text-xs">
                            {% if col.primary_key %}
                            <i data-lucide="key" class="w-3 h-3 text-yellow-500"></i>
                            {% else %}
                            <span class="w-3"></span>
                            {% endif %}
                            <span class="font-medium text-gray-700">{{ col.name }}</span>
                            <span class="text-gray-400">{{ col.type }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>

        <!-- Editor + Results -->
        <div class="flex-1 space-y-4">
            <!-- Editor -->
            <div class="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
                <div class="flex items-center justify-between mb-2">
                    <h2 class="text-sm font-bold text-gray-500 uppercase tracking-wider">Query</h2>
                    <div class="flex items-center gap-2">
                        <span id="query-status" class="text-xs text-gray-400"></span>
                        <button id="run-btn"
                                class="rounded-lg bg-teelo-lime px-4 py-2 text-sm font-bold text-teelo-dark hover:brightness-95 transition shadow-sm border border-black/5">
                            <span class="flex items-center gap-2">
                                <i data-lucide="play" class="w-4 h-4"></i> Run
                            </span>
                        </button>
                    </div>
                </div>
                <textarea id="sql-editor">SELECT * FROM players LIMIT 50;</textarea>
            </div>

            <!-- Mutation Preview -->
            <div id="preview-panel" class="hidden bg-amber-50 border border-amber-200 rounded-xl p-4 shadow-sm">
                <h3 class="text-sm font-bold text-amber-800 mb-2">Mutation Preview</h3>
                <p id="preview-info" class="text-sm text-amber-700 mb-3"></p>
                <div class="flex gap-2">
                    <button id="confirm-btn"
                            class="rounded-lg bg-red-600 px-4 py-2 text-sm font-bold text-white hover:bg-red-700 transition">
                        Confirm &amp; Execute
                    </button>
                    <button id="cancel-btn"
                            class="rounded-lg border border-gray-300 px-4 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-50 transition">
                        Cancel
                    </button>
                </div>
            </div>

            <!-- Results -->
            <div id="results-panel" class="hidden bg-white border border-gray-200 rounded-xl shadow-sm">
                <div class="flex items-center justify-between px-4 py-3 border-b border-gray-100">
                    <h2 class="text-sm font-bold text-gray-500 uppercase tracking-wider">Results</h2>
                    <span id="results-info" class="text-xs text-gray-400"></span>
                </div>
                <div class="results-table-wrap">
                    <table class="results-table w-full text-sm">
                        <thead id="results-head" class="bg-gray-50 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider"></thead>
                        <tbody id="results-body" class="divide-y divide-gray-100"></tbody>
                    </table>
                </div>
                <div id="pagination" class="hidden flex items-center justify-between px-4 py-3 border-t border-gray-100">
                    <button id="prev-page" class="rounded-lg border border-gray-300 px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 transition disabled:opacity-40" disabled>Previous</button>
                    <span id="page-info" class="text-xs text-gray-500"></span>
                    <button id="next-page" class="rounded-lg border border-gray-300 px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 transition disabled:opacity-40" disabled>Next</button>
                </div>
            </div>

            <!-- Error -->
            <div id="error-panel" class="hidden bg-red-50 border border-red-200 rounded-xl p-4">
                <p id="error-message" class="text-sm text-red-700"></p>
            </div>

            <!-- Success -->
            <div id="success-panel" class="hidden bg-green-50 border border-green-200 rounded-xl p-4">
                <p id="success-message" class="text-sm text-green-700"></p>
            </div>
        </div>
    </div>
</section>

<!-- Toast for copy -->
<div id="copy-toast" class="toast">Copied!</div>
{% endblock %}

{% block scripts %}
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.18/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.18/mode/sql/sql.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.18/addon/edit/matchbrackets.min.js"></script>
<script type="module" src="{{ url_for('static', path='/js/admin_sql.js') }}"></script>
{% endblock %}
```

**Step 2: Commit**

```bash
git add src/teelo/web/templates/admin_sql.html
git commit -m "feat: add SQL editor template"
```

---

### Task 6: Create the SQL editor JavaScript

**Files:**
- Create: `src/teelo/web/static/js/admin_sql.js`

**Step 1: Create the JS module**

```javascript
// Admin SQL Editor - client-side logic

const editor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
    mode: 'text/x-sql',
    lineNumbers: true,
    matchBrackets: true,
    indentWithTabs: false,
    tabSize: 2,
    autofocus: true,
    viewportMargin: Infinity,
});

// State
let currentPage = 1;
let currentQuery = '';
let sortColumn = null;
let sortDir = 'asc';

// DOM refs
const runBtn = document.getElementById('run-btn');
const statusEl = document.getElementById('query-status');
const resultsPanel = document.getElementById('results-panel');
const resultsHead = document.getElementById('results-head');
const resultsBody = document.getElementById('results-body');
const resultsInfo = document.getElementById('results-info');
const paginationEl = document.getElementById('pagination');
const prevBtn = document.getElementById('prev-page');
const nextBtn = document.getElementById('next-page');
const pageInfo = document.getElementById('page-info');
const errorPanel = document.getElementById('error-panel');
const errorMessage = document.getElementById('error-message');
const successPanel = document.getElementById('success-panel');
const successMessage = document.getElementById('success-message');
const previewPanel = document.getElementById('preview-panel');
const previewInfo = document.getElementById('preview-info');
const confirmBtn = document.getElementById('confirm-btn');
const cancelBtn = document.getElementById('cancel-btn');
const copyToast = document.getElementById('copy-toast');

function hideAll() {
    resultsPanel.classList.add('hidden');
    errorPanel.classList.add('hidden');
    successPanel.classList.add('hidden');
    previewPanel.classList.add('hidden');
}

function showError(msg) {
    hideAll();
    errorMessage.textContent = msg;
    errorPanel.classList.remove('hidden');
}

function showSuccess(msg) {
    hideAll();
    successMessage.textContent = msg;
    successPanel.classList.remove('hidden');
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function showToast(msg) {
    copyToast.textContent = msg;
    copyToast.classList.add('show');
    setTimeout(() => copyToast.classList.remove('show'), 1500);
}

async function runQuery(action = 'execute', page = 1) {
    const sql = editor.getValue().trim();
    if (!sql) return;

    currentQuery = sql;
    currentPage = page;
    statusEl.textContent = 'Running...';
    runBtn.disabled = true;

    try {
        const resp = await fetch('/admin/sql/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: sql, action, page }),
        });
        const data = await resp.json();

        if (!resp.ok) {
            showError(data.error || 'Query failed');
            statusEl.textContent = 'Error';
            return;
        }

        if (data.type === 'select') {
            renderResults(data);
            statusEl.textContent = `${data.total_rows} row${data.total_rows !== 1 ? 's' : ''} returned`;
        } else if (data.type === 'preview') {
            hideAll();
            previewInfo.textContent = `This query will affect ${data.affected_rows} row(s). Review and confirm to execute.`;
            previewPanel.classList.remove('hidden');
            statusEl.textContent = 'Preview';
        } else if (data.type === 'mutation') {
            showSuccess(`Query executed successfully. ${data.affected_rows} row(s) affected.`);
            statusEl.textContent = 'Done';
        }
    } catch (e) {
        showError('Network error: ' + e.message);
        statusEl.textContent = 'Error';
    } finally {
        runBtn.disabled = false;
    }
}

function renderResults(data) {
    hideAll();

    // Header
    resultsHead.innerHTML = '<tr>' + data.columns.map(col =>
        `<th class="px-4 py-2 cursor-pointer hover:text-teelo-dark select-none" data-col="${escapeHtml(col)}">${escapeHtml(col)} <span class="sort-indicator"></span></th>`
    ).join('') + '</tr>';

    // Body
    resultsBody.innerHTML = data.rows.map(row =>
        '<tr class="hover:bg-gray-50">' + row.map(cell => {
            const display = cell === null ? '<span class="text-gray-300 italic">NULL</span>' : escapeHtml(String(cell));
            return `<td class="px-4 py-2 whitespace-nowrap max-w-xs truncate">${display}</td>`;
        }).join('') + '</tr>'
    ).join('');

    resultsInfo.textContent = `${data.total_rows} row${data.total_rows !== 1 ? 's' : ''}`;
    resultsPanel.classList.remove('hidden');

    // Pagination
    if (data.total_pages > 1) {
        paginationEl.classList.remove('hidden');
        pageInfo.textContent = `Page ${data.page} of ${data.total_pages}`;
        prevBtn.disabled = data.page <= 1;
        nextBtn.disabled = data.page >= data.total_pages;
    } else {
        paginationEl.classList.add('hidden');
    }

    // Sort handlers
    resultsHead.querySelectorAll('th').forEach(th => {
        th.addEventListener('click', () => {
            const col = th.dataset.col;
            const colIdx = data.columns.indexOf(col);
            if (colIdx === -1) return;

            if (sortColumn === col) {
                sortDir = sortDir === 'asc' ? 'desc' : 'asc';
            } else {
                sortColumn = col;
                sortDir = 'asc';
            }

            data.rows.sort((a, b) => {
                let va = a[colIdx], vb = b[colIdx];
                if (va === null) return 1;
                if (vb === null) return -1;
                if (typeof va === 'number' && typeof vb === 'number') {
                    return sortDir === 'asc' ? va - vb : vb - va;
                }
                va = String(va); vb = String(vb);
                return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
            });
            renderResults({ ...data });
        });
    });

    // Copy on click
    resultsBody.querySelectorAll('td').forEach(td => {
        td.addEventListener('click', () => {
            const text = td.textContent;
            navigator.clipboard.writeText(text).then(() => showToast('Copied!'));
        });
    });
}

// Event listeners
runBtn.addEventListener('click', () => runQuery('execute'));
editor.setOption('extraKeys', {
    'Ctrl-Enter': () => runQuery('execute'),
    'Cmd-Enter': () => runQuery('execute'),
});

confirmBtn.addEventListener('click', () => runQuery('confirm'));
cancelBtn.addEventListener('click', () => {
    previewPanel.classList.add('hidden');
    statusEl.textContent = 'Cancelled';
});

prevBtn.addEventListener('click', () => runQuery('execute', currentPage - 1));
nextBtn.addEventListener('click', () => runQuery('execute', currentPage + 1));

// Schema browser
document.querySelectorAll('.schema-table-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const colsDiv = btn.nextElementSibling;
        const chevron = btn.querySelector('.schema-chevron');
        const isOpen = !colsDiv.classList.contains('hidden');

        colsDiv.classList.toggle('hidden');
        chevron.style.transform = isOpen ? '' : 'rotate(90deg)';
    });

    // Double-click to insert SELECT query
    btn.addEventListener('dblclick', () => {
        const table = btn.dataset.table;
        editor.setValue(`SELECT * FROM ${table} LIMIT 50;`);
        editor.focus();
    });
});

// Re-init lucide icons for dynamically created content
lucide.createIcons();
```

**Step 2: Commit**

```bash
git add src/teelo/web/static/js/admin_sql.js
git commit -m "feat: add SQL editor client-side JavaScript"
```

---

### Task 7: Add SQL editor link to admin home

**Files:**
- Modify: `src/teelo/web/templates/admin_home.html`

**Step 1: Add a card link**

Add a third card to the grid in `admin_home.html`:

```html
        <a href="/admin/sql"
           class="block bg-white border border-gray-200 rounded-xl p-5 shadow-sm hover:shadow transition">
            <div class="text-sm font-semibold text-gray-500">SQL Editor</div>
            <div class="mt-2 text-3xl font-bold text-teelo-dark">
                <i data-lucide="database" class="w-8 h-8"></i>
            </div>
            <div class="text-sm text-gray-500 mt-1">Query and update database tables</div>
        </a>
```

**Step 2: Commit**

```bash
git add src/teelo/web/templates/admin_home.html
git commit -m "feat: add SQL editor link to admin dashboard"
```

---

### Task 8: Rebuild Tailwind CSS

**Step 1: Rebuild**

```bash
cd /home/cammybeck/Documents/programming/Teelov4.0
npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify
```

**Step 2: Commit**

```bash
git add src/teelo/web/static/css/styles.css
git commit -m "chore: rebuild Tailwind CSS for SQL editor styles"
```

---

### Task 9: Manual verification

**Step 1: Start the dev server**

```bash
cd /home/cammybeck/Documents/programming/Teelov4.0
source venv/bin/activate
uvicorn teelo.api.main:app --reload
```

**Step 2: Test in browser**

1. Go to `/admin/login`, log in
2. Verify SQL Editor card appears on admin home
3. Click into SQL Editor
4. Verify schema sidebar shows tables with expandable columns
5. Run `SELECT * FROM players LIMIT 10;` — verify results table
6. Click a column header — verify sorting
7. Click a cell — verify copy-to-clipboard
8. Test pagination: `SELECT * FROM matches;` — verify page controls
9. Test mutation preview: `UPDATE players SET full_name = full_name WHERE id = 1;` — verify preview panel appears
10. Confirm mutation — verify success message
11. Test DDL blocking: `DROP TABLE players;` — verify error
12. Test Ctrl+Enter keyboard shortcut
13. Double-click a table in sidebar — verify query auto-fills

**Step 3: Check audit log**

```sql
SELECT * FROM admin_query_log ORDER BY executed_at DESC LIMIT 5;
```

Verify mutation queries were logged.
