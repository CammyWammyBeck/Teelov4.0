# Social Content Operations

Teelo stores X/Twitter content lifecycle records in the `social_content_*` tables and exposes them in the admin dashboard.

## Admin Views

- List: `/admin/tweet-activity`
- Detail: `/admin/tweet-activity/{item_id}`
- Filters: status, content type, workflow ID/text search, and posted tweet ID search

The dashboard is read-only. Operational changes still happen through the content agents and posting scripts.

## Current Write Paths

OpenClaw content scripts remain the primary mechanical queue/post/kill path while the database integration settles in. They dual-write best-effort events through:

```bash
~/.openclaw/workspace/scripts/social_content_db.py
```

That helper calls the Teelo bridge:

```bash
~/Teelov4.0/venv/bin/python ~/Teelov4.0/scripts/social_content_event.py
```

Supported events:

- `queued`
- `posted`
- `blocked`
- `failed`
- `killed`

Bridge failures print `DB_DUAL_WRITE_WARNING` and do not block queueing, posting, or killing. Set `TEELO_SOCIAL_CONTENT_DUAL_WRITE=0` to disable database dual-writes temporarily.

## Historical Backfill

Always review the candidate import before writing:

```bash
cd ~/Teelov4.0
venv/bin/python scripts/backfill_tweet_activity.py --summary
```

For full per-record inspection:

```bash
venv/bin/python scripts/backfill_tweet_activity.py
```

Live writes are intentionally guarded:

```bash
venv/bin/python scripts/backfill_tweet_activity.py --write --confirm-db-write
```

Do not run the write command against live Postgres without explicit Cam approval in the current conversation.

## Safety Rules

- Do not publish, delete, or alter social posts from the admin dashboard.
- Do not run live DB backfills without explicit approval.
- Preserve the distinction between broadcasts (`D-XXX`) and replies (`R-XXX`).
- Replies remain higher risk because they target specific people; they still require Cam approval.
- If the file-backed posting path and database disagree, treat the file-backed path as operational truth until Cam approves a full source-of-truth switch.

## Useful Checks

Confirm the schema revision:

```bash
venv/bin/alembic current --verbose
```

Run focused verification:

```bash
venv/bin/python -m pytest tests/unit/test_tweet_activity.py -q
venv/bin/python -m ruff check scripts/backfill_tweet_activity.py scripts/social_content_event.py src/teelo/services/social_content_writer.py tests/unit/test_tweet_activity.py
```
