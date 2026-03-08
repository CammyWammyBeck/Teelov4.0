# Admin SQL Editor & Table Viewer — Design

Date: 2026-03-08

## Overview

In-browser SQL editor in the admin section for viewing and updating database data. Located at `/admin/sql`.

## Layout

Three panels:
1. **Left sidebar** — Schema browser showing all tables, expandable to reveal columns with types
2. **Main area** — CodeMirror 6 SQL editor with syntax highlighting and a "Run" button
3. **Bottom area** — Results table with sortable columns, pagination, and row counts

## Query Flow

- **SELECT**: Execute immediately, render results in interactive table.
- **UPDATE/DELETE/INSERT**: Run inside a transaction, show affected row count + sample of affected rows. User confirms or cancels (commit or rollback).
- **DDL (ALTER, DROP, CREATE)**: Blocked entirely.

## Safeguards

- All mutations wrapped in transaction with preview step
- DDL statements rejected before execution
- Confirmation shows: query text, affected row count, sample rows
- Query timeout (30 seconds)

## Audit Log

New `AdminQueryLog` model:
- `id`, `admin_user_id` (FK), `query_text`, `query_type` (select/update/delete/insert), `affected_rows`, `success`, `error_message`, `executed_at`
- Only mutation queries logged (not SELECTs)

## Results Table

- Paginated (50 rows per page)
- Sortable columns (click header)
- Copy cell value on click
- Row count displayed

## Schema Browser

- Lists all tables from PostgreSQL `information_schema`
- Click table to expand columns (name, type, nullable, PK)
- Click table name to auto-insert `SELECT * FROM table_name LIMIT 50` into editor

## Tech Stack

- CodeMirror 6 from CDN
- Vanilla JS (consistent with codebase)
- `POST /admin/sql/execute` — accepts query, returns JSON results
- `GET /admin/sql/schema` — returns table/column metadata
- Jinja2 template extending `base.html`
