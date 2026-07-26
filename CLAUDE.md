# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Hatewatch is the data pipeline for the **India Hate Speech & Hate Crime Observatory**. This repo currently implements Step 1 of that pipeline: syncing incident reports submitted via a Jotform form into a PostgreSQL database. Step 2 (downloading/archiving media to Hetzner storage) is referenced in comments but not yet implemented anywhere in this repo.

## Commands

This project uses `uv` for dependency management (Python >= 3.14).

```bash
uv sync                      # install dependencies from pyproject.toml / uv.lock
uv run python test_jotform.py    # sanity-check Jotform API credentials/connectivity before syncing
uv run python jotform_sync.py    # incremental sync: only fetches submissions since the last run
uv run python jotform_sync.py --full   # full re-sync of all submissions from the beginning
```

There is no automated test suite (no pytest/unittest). `test_jotform.py` is a standalone manual diagnostic script — it prints results of 5 connectivity/data checks against the live Jotform API and is meant to be read by a human, not run in CI.

`main.py` is the unmodified `uv init` scaffold stub and is not part of the real pipeline.

### Required environment (`.env`)

`jotform_sync.py` and `test_jotform.py` both call `load_dotenv()` and hard-fail if these are missing:

- `JOTFORM_API_KEY`
- `JOTFORM_FORM_ID`
- `DATABASE_URL` (Postgres connection string; only required by `jotform_sync.py`)

### Known gap

`jotform_sync.py` imports `psycopg2`, but `psycopg2-binary` is **not** listed in `pyproject.toml` dependencies and is not installed in `.venv`. Running the sync will currently fail with `ModuleNotFoundError` until this is added (`uv add psycopg2-binary`).

## Architecture

`jotform_sync.py` is the entire pipeline, structured as a linear ETL:

1. **Fetch** (`fetch_submissions`) — paginates through the Jotform `/form/{id}/submissions` endpoint (1000/page), optionally filtered with `id:gt` for incremental sync.
2. **Flatten** (`extract_answers`) — Jotform returns answers as a nested `{"1": {"text": ..., "answer": ...}}` structure keyed by question ID; this flattens it to `{label: value}`, dropping blanks.
3. **Map** (`map_submission`) — the core transform, in two parts:
   - `FIELD_MAP` is a dict from exact Jotform question label → target DB column name. This mapping is label-based, not question-ID-based, so **if the Jotform form's question text changes, `FIELD_MAP` must be updated to match exactly** (case-sensitive).
   - A subset of labels map to `SPECIAL_FIELDS` (pseudo-columns like `__image_links__`, `__video_uploads__`, `__website_links__`) that aren't written to `incidents` directly — instead they're collected into a separate `media` dict of URL lists, later written to the `media_files` and `sources` tables.
   - After mapping, a battery of `normalise_*`/`to_*` helpers (dates, booleans, ints, arrays, enum-like status strings) clean up raw values, and several raw fields get consumed/removed via `row.pop(...)` and replaced with normalized fields (e.g. `fir_filed_raw` → `fir_status`).
   - `geolocation_raw` is regex-parsed as a fallback source for `city`/`state` when those fields are blank.
   - Any URLs embedded in the free-text `description` are also pulled out and added to `website_links` via `extract_urls_from_text`.
4. **Load** — three tables, all via raw `psycopg2` SQL (no ORM):
   - `incidents` — upserted via `upsert_incident`, keyed on `jotform_submission_id` (`ON CONFLICT ... DO UPDATE`). Only columns in `INCIDENT_COLUMNS` are written — **any mapped field not in that set is silently dropped**, so adding a new field to `FIELD_MAP` also requires adding its column name to `INCIDENT_COLUMNS` (and to `ARRAY_COLUMNS` if it's a Postgres array column).
   - `media_files` — one row per media URL (images/videos, both externally-linked and Jotform-hosted uploads), inserted with `archived = FALSE` as a queue for the not-yet-built Step 2 upload job. `_detect_platform` guesses the source platform from the URL host.
   - `sources` — one row per external website/news link cited as a source, `reliability = 'unverified'` by default.
5. **Sync bookkeeping** — `jotform_sync_log` records fetched/new/updated/error counts per run; `get_last_synced_id` reads the most recent `last_submission_id` from it to drive incremental syncs. Each submission is committed individually (`conn.commit()` per row) with per-row error isolation (`conn.rollback()` + logged, continues to next submission) — one bad submission does not abort the whole run.

None of the `incidents`/`media_files`/`sources`/`jotform_sync_log` table schemas live in this repo — they're assumed to pre-exist in the target Postgres database.

Logs go to both stdout and `sync.log` (`logging.FileHandler`).
