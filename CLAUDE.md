# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Hatewatch is the data pipeline for the **India Hate Speech & Hate Crime Observatory**. This repo currently implements Step 1 of that pipeline: syncing incident reports submitted via a Jotform form into a PostgreSQL database, plus a data-quality validation pass. Step 2 (downloading/archiving media to Hetzner storage) is referenced in comments but not yet implemented anywhere in this repo.

## Project structure

```
pipeline/     # ETL / processing scripts (jotform_sync.py, validate.py)
db/           # schema.sql + db/migrations/ (currently empty — no migrations yet)
tests/        # manual diagnostic scripts (test_jotform.py)
scripts/      # shell wrappers (pipeline.sh runs the full pipeline end-to-end)
logs/         # runtime logs (sync.log) — gitignored
reports/      # validate.py JSON output — gitignored
config/       # currently empty — reserved for future config files
review_ui/    # currently empty scaffold (templates/, static/) — no app code yet
```

## Commands

This project uses `uv` for dependency management (Python >= 3.14).

```bash
uv sync                                  # install dependencies from pyproject.toml / uv.lock
uv run python tests/test_jotform.py      # sanity-check Jotform API credentials/connectivity before syncing
uv run python pipeline/jotform_sync.py           # incremental sync: only fetches submissions since the last run
uv run python pipeline/jotform_sync.py --full    # full re-sync of all submissions from the beginning
uv run python pipeline/validate.py                        # dry-run data-quality report (no DB writes)
uv run python pipeline/validate.py --fix                  # apply suggested verification_status updates to the DB
uv run python pipeline/validate.py --report reports/latest_report.json  # save the report to JSON

bash scripts/pipeline.sh          # run the full pipeline: jotform_sync.py, then validate.py --report reports/latest_report.json
bash scripts/pipeline.sh --full   # same, but with a full Jotform re-sync
```

There is no automated test suite (no pytest/unittest). `tests/test_jotform.py` is a standalone manual diagnostic script — it prints results of 5 connectivity/data checks against the live Jotform API and is meant to be read by a human, not run in CI.

`main.py` (repo root) is the unmodified `uv init` scaffold stub and is not part of the real pipeline.

### Required environment (`.env`)

`pipeline/jotform_sync.py`, `pipeline/validate.py`, and `tests/test_jotform.py` all call `load_dotenv()` and hard-fail if required vars are missing:

- `JOTFORM_API_KEY` (jotform_sync.py, test_jotform.py)
- `JOTFORM_FORM_ID` (jotform_sync.py, test_jotform.py)
- `DATABASE_URL` — Postgres connection string (jotform_sync.py, validate.py)

## Architecture

### Known gap: `pipeline/jotform_sync.py` targets an old, narrower schema

`db/schema.sql` is the current 17-table schema (see its own header for the full design). `pipeline/jotform_sync.py`'s `FIELD_MAP`/`INCIDENT_COLUMNS`/`upsert_incident` logic has **not** been updated to match it — it still writes a flat, single-table version of `incidents` (no `incident_type`, `incident_severity`, `bias_motivation`, `state_code`, `latitude`/`longitude`, no `persons`/`organisations`/`legal_actions`/etc.). By contrast, `pipeline/validate.py` already queries/updates columns from the *new* schema (`incident_type`, `verification_status`, `latitude`, ...). Anyone touching either script should check which schema version it actually assumes before changing the other.

`pipeline/jotform_sync.py` is the ETL half of the pipeline, structured as a linear flow:

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

The table definitions themselves live in `db/schema.sql` (see the known-gap note above — `jotform_sync.py` does not fully match this file).

Logs go to both stdout and `logs/sync.log` (`logging.FileHandler`).

`pipeline/validate.py` is the second pipeline stage — a data-quality pass over the `incidents` table, run after sync:

1. Paginates through `incidents WHERE deleted_at IS NULL` in batches (`--batch`, default 500).
2. Runs each row through `CRITICAL_RULES` (missing date, future date, missing/too-short description, implausible casualties/injured counts) and `WARNING_RULES` (missing location, missing incident type, FIR/approval-status consistency checks) — see `validate_record`.
3. Derives a `suggested_status` for `verification_status` from the rule results plus the existing `approval_status` (never downgrades an already `verified`/`published`/`retracted` record).
4. Default mode is **dry-run** — reports only. `--fix` applies the suggested `verification_status` updates via batched `UPDATE`s (`psycopg2.extras.execute_batch`); `--limit N` caps how many records are scanned (for testing).
5. `--report PATH` writes a JSON summary (pass/fail counts, top issues by frequency, first 100 failed IDs) — `scripts/pipeline.sh` always passes `reports/latest_report.json`.
