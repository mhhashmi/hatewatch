#!/usr/bin/env python3
"""
import_jotform_csv.py — Import JotForm CSV export into raw_incidents
=====================================================================
One-time (and periodic) import of the full JotForm CSV export.
This is the ONLY way to get Approval Status into the DB since
JotForm does not expose it via the API.

Writes to:
  - raw_incidents  (all fields including jotform_approval)

Usage:
  uv run python pipeline/import_jotform_csv.py path/to/export.csv
  uv run python pipeline/import_jotform_csv.py path/to/export.csv --fix
  uv run python pipeline/import_jotform_csv.py path/to/export.csv --fix --batch 500

Options:
  --fix          Write to DB (default: dry-run)
  --batch N      Batch size for DB inserts (default: 200)
  --limit N      Only process first N rows (for testing)
"""

import os
import re
import sys
import csv
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise SystemExit('ERROR: DATABASE_URL missing from .env')

# ---------------------------------------------------------------------------
# CSV column → raw_incidents column mapping
# Based on exact column headers from JotForm CSV export
# ---------------------------------------------------------------------------
CSV_COLUMN_MAP = {
    'Submission ID':                    'jotform_submission_id',
    'Date':                             'incident_date_raw',
    'Approval Status':                  'jotform_approval',
    'Submission Date':                  'jotform_created_at',
    'Last Update Date':                 'jotform_updated_at',
    'Title':                            'title',
    'Website links':                    '__website_links__',
    'Source of information':            'source_of_information',
    'Investigating agency':             'investigating_agency',
    'FIR filed':                        'fir_filed_raw',
    'Police station':                   'police_station',
    'Upload images(s)':                 '__image_uploads__',
    'FIR filed date':                   'fir_filed_date_raw',
    'The hate crime included':          'hate_crime_included_raw',
    'Submission IP':                    'submission_ip',
    'Motive/Cause':                     'motive_cause',
    'Type of venue':                    'type_of_venue_raw',
    'Link to images':                   '__image_links__',
    'Timer':                            None,              # drop - form fill time
    'Video by':                         'video_by',
    'Link to videos':                   '__video_links__',
    'Victim Details':                   'victim_details_raw',
    'Suspect Details':                  'suspect_details_raw',
    'Other current status':             'other_current_status',
    'Online images':                    '__image_links__',
    'FIR Charges 4':                    'fir_charges_4',
    'Online videos':                    '__video_links__',
    'Tag this incident':                'tags_raw',
    'City':                             'city',
    'FIR Charges 5':                    'fir_charges_5',
    'Nature of incident':               'nature_of_incident',
    'State':                            'state',
    'Cross FIR Charges 2':              'cross_fir_charges_2',
    'Other source of information':      'other_source',
    'The hate speech was made by a':    'hate_speech_made_by',
    'Type of discrimination':           'type_of_discrimination_raw',
    'Cross FIR Charges 3':              'cross_fir_charges_3',
    'Police role':                      'police_role_raw',
    'Number of suspects':               'number_of_suspects',
    'Address of the incident location': 'address_raw',
    'Source App':                       'source_app',
    'Online harassment incident':       'online_harassment_raw',
    'Cross FIR Charges 4':              'cross_fir_charges_4',
    'Casualties':                       'casualties_raw',
    'Image source':                     'image_source',
    'Upload images/videos':             '__image_uploads__',
    'Postal Code':                      'postal_code',
    'FIR Charges 5':                    'fir_charges_5',   # duplicate col handled below
    'Injured':                          'injured_raw',
    'Images by':                        'images_by',
    'Add Images':                       '__image_uploads__',
    'Geolocation':                      'geolocation_raw',
    'Street':                           'street',
    'Harassed':                         'harassed_raw',
    'Add videos':                       '__video_uploads__',
    'House Number':                     'house_number',
    'FIR Charges':                      'fir_charges_1',
    'Video source':                     'video_source',
    'Investigating officer':            'investigating_officer',
    'FIR Charges 2':                    'fir_charges_2',
    'Other State Govt. Party':          'other_state_party',
    'Upload video(s)':                  '__video_uploads__',
    'FIR Charges 3':                    'fir_charges_3',
    'Description':                      'description',
    'Cross FIR filed against':          'cross_fir_filed_against',
    'State Government party':           'state_government_party',
    'Other motive or cause':            'other_motive',
    'Cross FIR Charges 1':              'cross_fir_charges_1',
    'Other investigating agency':       'other_investigating_agency',
    'FIR filed against':                'fir_filed_against',
    'Case current status':              'case_current_status',
    'Other type of venue':              'other_venue_type',
    'Cross FIR filed':                  'cross_fir_filed_raw',
    'Cross FIR filed date':             'cross_fir_filed_date_raw',
    'No Label':                         None,              # drop - unlabelled fields
    'Displaced':                        'displaced_raw',
    'Property damage':                  'property_damage_raw',
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_date(val: str) -> Optional[str]:
    if not val or not val.strip():
        return None
    val = val.strip()
    for fmt in (
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
        '%B %d, %Y',          # May 1, 2026
        '%d/%m/%Y',           # 27/06/2026
        '%m/%d/%Y',
        '%d-%m-%Y',
        '%b %d, %Y',          # Jun 27, 2026
    ):
        try:
            return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def clean_value(val: str) -> Optional[str]:
    """Strip whitespace; return None for empty/placeholder values."""
    if not val:
        return None
    v = val.strip()
    if v.lower() in ('', 'n/a', 'na', 'none', 'null', '-'):
        return None
    return v


def split_multiline(val: str) -> list[str]:
    """Split newline-separated URLs or values into a list."""
    if not val:
        return []
    return [u.strip() for u in val.split('\n') if u.strip()]


def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    return re.findall(r'https?://[^\s\'"<>]+', text)


def normalise_approval(val: str) -> Optional[str]:
    if not val or not val.strip():
        return None
    s = val.strip().lower()
    if s in ('approved', 'completed'):
        return 'approved'
    if s in ('denied', 'rejected'):
        return 'rejected'
    if s in ('in progress', 'in_progress'):
        return 'in_progress'
    return val.strip()  # preserve unknown values as-is


# ---------------------------------------------------------------------------
# Row mapper
# ---------------------------------------------------------------------------

def map_csv_row(row: dict, col_positions: dict) -> tuple[dict, dict]:
    """
    Map a CSV row to (raw_row, media).
    Handles duplicate column names by position.
    """
    raw   = {}
    media = {'image_urls': [], 'video_urls': [], 'website_urls': []}

    # Track which FIR charges slot we're filling (CSV has duplicate col names)
    fir_charges_seen    = 0
    cross_fir_seen      = 0
    police_station_seen = 0
    investigating_officer_seen = 0
    no_label_seen       = 0

    for col_name, value in row.items():
        value = value.strip() if value else ''

        # Handle duplicate column names
        if col_name == 'No Label':
            no_label_seen += 1
            continue  # always drop

        if col_name == 'Police station':
            police_station_seen += 1
            if police_station_seen == 1:
                raw['police_station'] = clean_value(value)
            continue

        if col_name == 'Investigating officer':
            investigating_officer_seen += 1
            if investigating_officer_seen == 1:
                raw['investigating_officer'] = clean_value(value)
            continue

        if col_name == 'FIR Charges 5':
            fir_charges_seen += 1
            slot = f'fir_charges_{4 + fir_charges_seen}'
            raw[slot] = clean_value(value)
            continue

        target = CSV_COLUMN_MAP.get(col_name)
        if target is None:
            continue  # explicitly dropped

        if not value:
            continue

        # Route media
        if target == '__image_uploads__' or target == '__image_links__':
            media['image_urls'].extend(split_multiline(value))
            continue
        if target == '__video_uploads__' or target == '__video_links__':
            media['video_urls'].extend(split_multiline(value))
            continue
        if target == '__website_links__':
            media['website_urls'].extend(split_multiline(value))
            continue

        raw[target] = value

    # Post-process
    sub_id = raw.get('jotform_submission_id', '')

    # Natural key — same as API sync so CSV merges with existing API row
    # Format: jotform_api:{id} matches what jotform_sync.py creates
    raw['raw_id']       = f'jotform_api:{sub_id}'
    raw['source_type']  = 'jotform_csv'   # recorded but won't overwrite API source_type
    raw['source_system']= 'jotform'

    # Normalise approval
    if raw.get('jotform_approval'):
        raw['jotform_approval'] = normalise_approval(raw['jotform_approval'])

    # Parse dates
    raw['incident_date'] = to_date(raw.get('incident_date_raw', ''))
    raw['fir_filed_date'] = to_date(raw.get('fir_filed_date_raw', ''))
    raw['cross_fir_filed_date'] = to_date(raw.get('cross_fir_filed_date_raw', ''))

    # Collect FIR charges
    fir_charges = [raw.pop(f'fir_charges_{i}', None) for i in range(1, 6)]
    fir_charges = [c for c in fir_charges if c and c.strip()]
    raw['fir_charges_raw'] = fir_charges if fir_charges else None

    # Collect Cross FIR charges
    cross_charges = [raw.pop(f'cross_fir_charges_{i}', None) for i in range(1, 5)]
    cross_charges = [c for c in cross_charges if c and c.strip()]
    raw['cross_fir_charges_raw'] = cross_charges if cross_charges else None

    # Parse hate_crime_included (comma-separated in CSV)
    hci = raw.pop('hate_crime_included_raw', None)
    if hci:
        raw['hate_crime_included'] = [v.strip() for v in hci.split(',') if v.strip()]

    # Parse tags (comma-separated in CSV)
    tags = raw.pop('tags_raw', None)
    if tags:
        raw['tags'] = [v.strip() for v in tags.split(',') if v.strip()]

    # Parse type_of_venue
    tov = raw.pop('type_of_venue_raw', None)
    if tov:
        raw['type_of_venue'] = [v.strip() for v in tov.split(',') if v.strip()]

    # Parse type_of_discrimination
    tod = raw.pop('type_of_discrimination_raw', None)
    if tod:
        raw['type_of_discrimination'] = [v.strip() for v in tod.split(',') if v.strip()]

    # Store media URLs as raw text
    raw['image_urls_raw']   = '\n'.join(filter(None, media['image_urls'])) or None
    raw['video_urls_raw']   = '\n'.join(filter(None, media['video_urls'])) or None
    raw['website_urls_raw'] = '\n'.join(filter(None, media['website_urls'])) or None

    # Extract URLs from description too
    desc_urls = extract_urls(raw.get('description', ''))
    if desc_urls:
        existing = raw.get('website_urls_raw') or ''
        all_urls = split_multiline(existing) + desc_urls
        raw['website_urls_raw'] = '\n'.join(dict.fromkeys(filter(None, all_urls))) or None

    # Clean None values
    raw = {k: v for k, v in raw.items() if v is not None and v != '' and v != []}

    return raw, media


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def batch_upsert_raw(conn, rows: list[dict]) -> tuple[int, int]:
    """
    Upsert into raw_incidents.
    CSV data wins for jotform_approval.
    API data wins for everything else (COALESCE).
    """
    if not rows:
        return 0, 0

    all_cols = list({col for row in rows for col in row.keys()})

    # jotform_approval from CSV always wins — overwrite API value
    # source_type: keep existing API value if present
    # Everything else: COALESCE (don't overwrite existing API data)
    CSV_WINS    = {'jotform_approval'}
    PROTECTED   = {'processed', 'processed_at', 'incident_id', 'processing_notes',
                   'source_type'}  # keep 'jotform_api' if already set
    COALESCE    = set(all_cols) - {'raw_id'} - CSV_WINS - PROTECTED

    parts = []
    parts += [f'{c} = EXCLUDED.{c}' for c in all_cols if c in CSV_WINS]
    parts += [f'{c} = COALESCE(raw_incidents.{c}, EXCLUDED.{c})'
              for c in all_cols if c in COALESCE]
    parts += [f'{c} = COALESCE(raw_incidents.{c}, EXCLUDED.{c})'
              for c in all_cols if c in PROTECTED]
    update_set = ', '.join(parts)

    ph      = ', '.join(['%s'] * len(all_cols))
    col_str = ', '.join(all_cols)

    sql = f"""
        INSERT INTO raw_incidents ({col_str})
        VALUES ({ph})
        ON CONFLICT (raw_id)
        DO UPDATE SET {update_set},
                      updated_at = NOW()
        RETURNING id, (xmax = 0) AS is_insert
    """

    val_tuples = [tuple(row.get(col) for col in all_cols) for row in rows]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, val_tuples, page_size=100)
        results = cur.fetchall()

    new     = sum(1 for _, is_ins in results if is_ins)
    updated = sum(1 for _, is_ins in results if not is_ins)
    return new, updated


# ---------------------------------------------------------------------------
# Also update jotform_approval on incidents table
# ---------------------------------------------------------------------------

def sync_approval_to_incidents(conn, rows: list[dict]) -> int:
    """
    Copy jotform_approval from raw_incidents to incidents.jotform_approval
    for records that are already processed.
    """
    updates = [
        (r['jotform_approval'], r['jotform_submission_id'])
        for r in rows
        if r.get('jotform_approval') and r.get('jotform_submission_id')
    ]
    if not updates:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE incidents
            SET jotform_approval = %s,
                updated_at = NOW()
            WHERE jotform_submission_id = %s
            AND (jotform_approval IS NULL OR jotform_approval = 'pending')
            """,
            updates,
            page_size=500,
        )
    return len(updates)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(csv_path: str, dry_run: bool, batch_size: int, limit: Optional[int]):
    mode = 'DRY RUN' if dry_run else 'LIVE'
    log.info('=== import_jotform_csv.py started (%s) ===', mode)
    log.info('File: %s', csv_path)

    # Read CSV
    rows_raw = []
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            rows_raw.append(dict(row))

    log.info('Read %d rows from CSV', len(rows_raw))

    # Map rows
    mapped_rows = []
    media_list  = []
    errors      = 0

    for row in rows_raw:
        try:
            raw_row, media = map_csv_row(row, {})
            if raw_row.get('jotform_submission_id'):
                mapped_rows.append(raw_row)
                media_list.append(media)
        except Exception as e:
            errors += 1
            log.error('MAP ERROR row %s: %s',
                      row.get('Submission ID', '?'), e)

    log.info('Mapped %d rows (%d errors)', len(mapped_rows), errors)

    # Show approval distribution
    from collections import Counter
    approvals = Counter(r.get('jotform_approval', 'unknown') for r in mapped_rows)
    print()
    print('=' * 55)
    print(f'  JotForm CSV Import ({mode})')
    print('=' * 55)
    print(f'  File:          {csv_path}')
    print(f'  Total rows:    {len(rows_raw):,}')
    print(f'  Mapped OK:     {len(mapped_rows):,}')
    print(f'  Errors:        {errors}')
    print()
    print('  Approval Status distribution:')
    for status, count in sorted(approvals.items(), key=lambda x: -x[1]):
        print(f'    {status or "(empty)":<20} {count:>6,}')
    print()

    if dry_run:
        print('  Sample of first 3 mapped rows:')
        for r in mapped_rows[:3]:
            print(f'    {r["jotform_submission_id"]} | '
                  f'{r.get("jotform_approval","?")} | '
                  f'{r.get("incident_date","?")} | '
                  f'{(r.get("title") or r.get("description",""))[:50]}')
        print()
        print('  Run with --fix to import to DB')
        print('=' * 55)
        return

    # Import to DB
    conn = psycopg2.connect(DATABASE_URL)
    total_new = total_updated = 0

    for i in range(0, len(mapped_rows), batch_size):
        batch = mapped_rows[i:i + batch_size]
        try:
            new, updated = batch_upsert_raw(conn, batch)
            total_new     += new
            total_updated += updated
            conn.commit()
            log.info('Batch %d–%d: +%d new, ~%d updated',
                     i + 1, i + len(batch), new, updated)
        except Exception as e:
            conn.rollback()
            log.error('BATCH ERROR at offset %d: %s', i, e)

    # Sync approval to incidents table
    approval_updated = sync_approval_to_incidents(conn, mapped_rows)
    conn.commit()
    log.info('Synced jotform_approval to %d incidents records', approval_updated)

    print(f'  Imported to raw_incidents:')
    print(f'    New records:     {total_new:,}')
    print(f'    Updated records: {total_updated:,}')
    print(f'    Approval synced to incidents: {approval_updated:,}')
    print('=' * 55)
    print()

    conn.close()
    log.info('=== import_jotform_csv.py complete ===')
    log.info('Next: run process.py to enrich raw_incidents → incidents')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Import JotForm CSV export into raw_incidents'
    )
    parser.add_argument('csv_path', help='Path to JotForm CSV export file')
    parser.add_argument('--fix',    action='store_true',
                        help='Write to DB (default: dry-run)')
    parser.add_argument('--batch',  type=int, default=200,
                        help='Batch size (default: 200)')
    parser.add_argument('--limit',  type=int, default=None,
                        help='Only process first N rows (for testing)')
    args = parser.parse_args()

    run(
        csv_path  = args.csv_path,
        dry_run   = not args.fix,
        batch_size= args.batch,
        limit     = args.limit,
    )