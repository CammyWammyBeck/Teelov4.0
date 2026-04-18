# Teelo

Teelo is a tennis analytics platform for match predictions, ELO rankings, and betting analysis. The repo combines data ingestion, Postgres-backed state, rating and feature pipelines, model training, and a FastAPI web app for browsing matches, rankings, players, tournaments, and admin dashboards.

## Stack at a glance

- Python 3.11+
- FastAPI for the web app in `src/teelo/web/`
- SQLAlchemy, Alembic, and PostgreSQL for persistence
- Playwright-based scraping and ingestion tooling
- ELO, feature engineering, and ML pipelines under `src/teelo/elo/`, `src/teelo/features/`, and `src/teelo/ml/`
- Tailwind CSS for frontend styling

## Project layout

The main code lives under `src/teelo/`:

- `web/`: FastAPI app, routers, templates, static assets, and web-facing services. App entrypoint: `src/teelo/web/main.py`.
- `db/`: database models, sessions, and persistence wiring.
- `scrape/`: ATP/WTA scraping, queueing, and ingestion helpers.
- `elo/`: rating calculations and live ELO state updates.
- `features/`: feature registry and feature backfill pipeline.
- `ml/`: training, evaluation, prediction, selection, and model versioning.
- `services/` and `tasks/`: orchestration code for ingestion, predictions, metrics, notifications, and scheduled work.
- `players/`, `notifications/`, `utils/`, `api/`: supporting domain and integration modules.

Tests primarily live in `tests/`, with frontend JavaScript tests in `src/teelo/web/static/js/tests/`.

## Local development

The default local setup uses a Python virtual environment in the repo root.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
npm install
cp .env.example .env
```

Then update `.env` for your local database and any scraper/admin settings you need. The example file includes `DATABASE_URL`, scraper display settings, and admin session configuration. `npm install` provides the Tailwind dependency used by `teelo css`.

## Common commands

Run the web app:

```bash
teelo web --host 127.0.0.1 --port 8000
```

Build CSS once:

```bash
teelo css
```

Watch and rebuild CSS during frontend work:

```bash
teelo css --watch
```

Apply database migrations:

```bash
alembic upgrade head
```

Create a new migration after schema changes:

```bash
alembic revision --autogenerate -m "describe-change"
```

Run the verification suite:

```bash
pytest
ruff check .
black --check .
mypy src
```

Some useful pipeline commands exposed through the CLI:

```bash
teelo features-backfill
teelo predictions-backfill
teelo predictions-live
teelo retrain
```

## Docs map

- `docs/feature-reference.md`: complete ML feature catalog and feature semantics.
- `docs/elo-operations.md`: ELO tuning, activation, and live-state rebuild operations.
- `docs/server-setup-arch.md`: Arch server setup for headless scraper/backfill workers.
- `docs/server-setup-docker.md`: Docker-based server setup and update flow.
- `src/teelo/web/README.md`: module boundaries and ownership rules for the web layer.

If you are orienting yourself to the codebase for the first time, start with this README, then the web module boundaries doc and the feature/ELO references depending on the area you are touching.
