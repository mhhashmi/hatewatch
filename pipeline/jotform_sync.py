#!/usr/bin/env python3
"""
jotform_sync.py — HateWatch JotForm → PostgreSQL sync
======================================================
Syncs incident reports from JotForm into the 17-table HateWatch schema.

Writes to:
  - incidents        (one row per submission)
  - media_files      (images / videos, archived=FALSE queue for Hetzner)
  - sources          (website links cited as evidence)
  - jotform_sync_log (run metadata)

Usage:
  uv run python jotform_sync.py            # incremental (since last sync)
  uv run python jotform_sync.py --full     # full re-sync of all submissions

Environment (.env):
  JOTFORM_API_KEY
  JOTFORM_FORM_ID
  DATABASE_URL
"""

import os
import re
import sys
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging — stdout + sync.log
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sync.log'),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
JOTFORM_API_KEY = os.environ.get('JOTFORM_API_KEY')
JOTFORM_FORM_ID = os.environ.get('JOTFORM_FORM_ID')
DATABASE_URL    = os.environ.get('DATABASE_URL')

if not JOTFORM_API_KEY or not JOTFORM_FORM_ID or not DATABASE_URL:
    raise SystemExit('ERROR: JOTFORM_API_KEY, JOTFORM_FORM_ID, and DATABASE_URL must be set in .env')

BASE_URL = 'https://api.jotform.com'
HEADERS  = {'APIKEY': JOTFORM_API_KEY}
PAGE_SIZE = 1000

# ---------------------------------------------------------------------------
# FIELD_MAP — Jotform question label → incidents column
# Special values starting with __ are routed to other tables.
# ---------------------------------------------------------------------------
FIELD_MAP = {
    # ── Core incident ──────────────────────────────────────────────────────
    'Date':                             'incident_date',
    'Date of incident':                 'incident_date',
    'Title':                            'title',
    'Description':                      'description',
    'Nature of incident':               'nature_of_incident',
    'The hate crime included':          'hate_crime_included',
    'Motive/Cause':                     'motive_cause',
    'Other motive or cause':            'other_motive',
    'Type of venue':                    'type_of_venue',
    'Other type of venue':              'other_venue_type',
    'Type of discrimination':           'type_of_discrimination',
    'Tag this incident':                'tags',

    # ── Location ───────────────────────────────────────────────────────────
    'State':                            'state',
    'City':                             'city',
    'Address of the incident location': 'address',
    'Street':                           'street',
    'House Number':                     'house_number',
    'Postal Code':                      'postal_code',
    'Geolocation':                      'geolocation_raw',

    # ── Source / reporter ──────────────────────────────────────────────────
    'Source of information':            'source_of_information',
    'Other source of information':      'other_source',
    'Source App':                       'source_app',
    'Website links':                    '__website_links__',
    'Online images':                    '__image_links__',
    'Online videos':                    '__video_links__',
    'Link to images':                   '__image_links__',
    'Link to videos':                   '__video_links__',
    'Upload images(s)':                 '__image_uploads__',
    'Upload video(s)':                  '__video_uploads__',
    'Add Images':                       '__image_uploads__',
    'Add videos':                       '__video_uploads__',
    'Upload images/videos':             '__image_uploads__',
    'Image source':                     'image_source',
    'Video source':                     'video_source',
    'Images by':                        'images_by',
    'Video by':                         'video_by',

    # ── Victim details ─────────────────────────────────────────────────────
    'Victim Details':                   'victim_details_raw',
    'Casualties':                       'casualties_raw',
    'Injured':                          'injured_raw',
    'Harassed':                         'harassed_raw',
    'Displaced':                        'displaced_raw',
    'Property damage':                  'property_damage_raw',

    # ── Suspect / perpetrator ──────────────────────────────────────────────
    'Suspect Details':                  'suspect_details_raw',
    'Number of suspects':               'number_of_suspects',
    'The hate speech was made by a':    'hate_speech_made_by',

    # ── FIR / police ───────────────────────────────────────────────────────
    'FIR filed':                        'fir_filed_raw',
    'FIR filed date':                   'fir_filed_date',
    'FIR Charges':                      'fir_charges_1',
    'FIR Charges 2':                    'fir_charges_2',
    'FIR Charges 3':                    'fir_charges_3',
    'FIR Charges 4':                    'fir_charges_4',
    'FIR Charges 5':                    'fir_charges_5',
    'FIR filed against':                'fir_filed_against',
    'Police station':                   'police_station',
    'Police role':                      'police_role_raw',
    'Investigating agency':             'investigating_agency',
    'Other investigating agency':       'other_investigating_agency',
    'Investigating officer':            'investigating_officer',
    'Case current status':              'case_current_status',
    'Other current status':             'other_current_status',

    # ── Cross FIR ──────────────────────────────────────────────────────────
    'Cross FIR filed':                  'cross_fir_filed_raw',
    'Cross FIR filed date':             'cross_fir_filed_date',
    'Cross FIR filed against':          'cross_fir_filed_against',
    'Cross FIR Charges 1':              'cross_fir_charges_1',
    'Cross FIR Charges 2':              'cross_fir_charges_2',
    'Cross FIR Charges 3':              'cross_fir_charges_3',
    'Cross FIR Charges 4':              'cross_fir_charges_4',

    # ── Political context ──────────────────────────────────────────────────
    'State Government party':           'state_government_party',
    'Other State Govt. Party':          'other_state_party',

    # ── Online harassment ──────────────────────────────────────────────────
    'Online harassment incident':       'online_harassment_raw',

    # ── Workflow ───────────────────────────────────────────────────────────
    'Approval Status':                  'approval_status_raw',
    'Submission IP':                    'submission_ip',
}

# ---------------------------------------------------------------------------
# INCIDENT_COLUMNS — columns that exist in the new incidents table
# Any mapped field NOT in this set is silently dropped.
# ---------------------------------------------------------------------------
INCIDENT_COLUMNS = {
    # identifiers
    'jotform_submission_id',
    # core
    'incident_type', 'incident_severity', 'bias_motivation',
    'nature_of_incident', 'description', 'tags',
    'incident_date', 'incident_date_approx',
    # location
    'address', 'city', 'district', 'state', 'state_code',
    'postal_code', 'latitude', 'longitude',
    'type_of_venue', 'type_of_discrimination',
    # victim
    'casualties', 'injured', 'displaced', 'harassed',
    'property_damage', 'online_harassment', 'online_harassment_detail',
    'victim_details_raw',
    # perpetrator
    'number_of_suspects', 'hate_speech_made_by',
    'perpetrator_type', 'suspect_details_raw',
    # FIR / police
    'fir_status', 'fir_number', 'fir_filed_date', 'fir_filed_against',
    'police_station', 'ps_code', 'police_role',
    'investigating_agency', 'other_investigating_agency', 'investigating_officer',
    'case_current_status', 'fir_charges', 'hate_crime_included',
    # cross FIR
    'cross_fir_filed', 'cross_fir_filed_date',
    'cross_fir_filed_against', 'cross_fir_charges',
    # political context
    'state_government_party',
    # raw / metadata
    'geolocation_raw', 'submission_ip', 'approval_status',
    # provenance (set automatically)
    'data_source', 'verification_status', 'reliability_level',
    'published', 'sensitive',
}

# Postgres array columns — must be wrapped in a list before insert
ARRAY_COLUMNS = {
    'hate_crime_included', 'tags', 'fir_charges',
    'cross_fir_charges', 'type_of_venue', 'type_of_discrimination',
}

# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def to_date(val) -> Optional[str]:
    """Convert various date formats to ISO YYYY-MM-DD.
    Handles JotForm dict format: {'day': '24', 'month': '07', 'year': '2026', ...}
    """
    if not val:
        return None
    # JotForm sends dates as dicts
    if isinstance(val, dict):
        # prefer the 'datetime' key if present
        if val.get('datetime'):
            val = val['datetime']
        elif all(k in val for k in ('year', 'month', 'day')):
            return f"{val['year']}-{int(val['month']):02d}-{int(val['day']):02d}"
        else:
            return None
    val = str(val).strip()
    if not val:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%B %d, %Y'):
        try:
            return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None  # drop unparseable dates rather than passing bad data to DB


def to_bool(val) -> Optional[bool]:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ('yes', 'true', '1', 'y'):
        return True
    if s in ('no', 'false', '0', 'n'):
        return False
    return None


def to_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def to_array(val) -> Optional[list]:
    if val is None:
        return None
    if isinstance(val, list):
        return [str(v).strip() for v in val if v]
    s = str(val).strip()
    if not s:
        return None
    if ',' in s:
        return [v.strip() for v in s.split(',') if v.strip()]
    return [s]


def normalise_fir_status(val) -> Optional[str]:
    if not val:
        return 'unknown'
    s = str(val).strip().lower()
    if any(x in s for x in ('yes', 'filed', 'true')):
        return 'filed'
    if any(x in s for x in ('no', 'not', 'false')):
        return 'not_filed'
    return 'unknown'


def normalise_police_role(val) -> Optional[str]:
    if not val:
        return 'unknown'
    s = str(val).strip().lower()
    mapping = {
        'complicit':          'complicit',
        'failed':             'failed_to_act',
        'delayed':            'delayed_action',
        'acted':              'acted_appropriately',
        'appropriate':        'acted_appropriately',
        'cross fir':          'filed_cross_fir',
        'cross_fir':          'filed_cross_fir',
    }
    for key, norm in mapping.items():
        if key in s:
            return norm
    return 'unknown'


def normalise_approval_status(val) -> Optional[str]:
    if not val:
        return 'pending'
    s = str(val).strip().lower()
    if 'approved' in s:
        return 'approved'
    if 'denied' in s or 'rejected' in s:
        return 'rejected'
    if 'in progress' in s or 'in_progress' in s or 'review' in s:
        return 'in_review'
    if 'needs more' in s or 'needs_more' in s:
        return 'needs_more_info'
    return 'pending'


def extract_urls_from_text(text: str) -> list:
    if not text:
        return []
    return re.findall(r'https?://[^\s\'"<>]+', text)


def parse_geolocation(raw: str):
    """Extract lat/lng from 'lat:28.123 lng:77.456' or similar."""
    if not raw:
        return None, None
    lat_m = re.search(r'lat(?:itude)?[:\s]+(-?\d+\.?\d*)', raw, re.I)
    lng_m = re.search(r'l(?:on|ng)(?:g|itude)?[:\s]+(-?\d+\.?\d*)', raw, re.I)
    lat = float(lat_m.group(1)) if lat_m else None
    lng = float(lng_m.group(1)) if lng_m else None
    return lat, lng


def detect_platform(url: str) -> str:
    if not url:
        return 'unknown'
    url = url.lower()
    for platform in ('instagram', 'facebook', 'youtube', 'whatsapp',
                     'telegram', 'twitter', 'x.com', 'tiktok'):
        if platform in url:
            return platform.replace('x.com', 'twitter')
    return 'other'


# ---------------------------------------------------------------------------
# JotForm API
# ---------------------------------------------------------------------------

def fetch_submissions(since_id: Optional[str] = None) -> list:
    """Paginate through all submissions, optionally filtered by ID."""
    submissions = []
    offset = 0
    while True:
        params = {
            'limit':    PAGE_SIZE,
            'offset':   offset,
            'orderby':  'id',
            'direction': 'ASC',
        }
        if since_id:
            params['filter'] = json.dumps({'id:gt': since_id})

        r = requests.get(
            f'{BASE_URL}/form/{JOTFORM_FORM_ID}/submissions',
            headers=HEADERS,
            params=params,
            timeout=30,
        )
        data = r.json()
        if data.get('responseCode') != 200:
            log.error('JotForm API error: %s', data.get('message'))
            break

        batch = data.get('content', [])
        if not batch:
            break

        submissions.extend(batch)
        log.info('Fetched %d submissions (offset %d)', len(batch), offset)

        offset += len(batch)
        # if we got fewer than a full page, we're done
        if len(batch) < PAGE_SIZE:
            break

    return submissions


def extract_answers(submission: dict) -> dict:
    """Flatten Jotform's nested answer structure to {label: value}."""
    flat = {}
    for qid, q in submission.get('answers', {}).items():
        label  = q.get('text', '').strip()
        answer = q.get('answer')
        if answer is None or answer == '' or answer == []:
            continue
        if label and label not in ('Submit', 'Page Break', 'undefined'):
            flat[label] = answer
    return flat


# ---------------------------------------------------------------------------
# Mapping — raw answers → DB row
# ---------------------------------------------------------------------------

def map_submission(sub: dict, answers: dict) -> tuple[dict, dict]:
    """
    Map a JotForm submission to (incident_row, media_dict).

    Returns:
        incident_row  — dict of columns for the incidents table
        media         — dict with keys: image_urls, video_urls, website_urls
    """
    row   = {}
    media = {'image_urls': [], 'video_urls': [], 'website_urls': []}

    # ── Map all fields ────────────────────────────────────────────────────
    for label, value in answers.items():
        col = FIELD_MAP.get(label)
        if not col:
            continue

        if col == '__image_links__':
            urls = to_array(value) or []
            media['image_urls'].extend(urls)
        elif col == '__video_links__':
            urls = to_array(value) or []
            media['video_urls'].extend(urls)
        elif col == '__image_uploads__':
            if isinstance(value, list):
                media['image_urls'].extend([v.get('url', '') for v in value if isinstance(v, dict)])
            elif isinstance(value, str):
                media['image_urls'].append(value)
        elif col == '__video_uploads__':
            if isinstance(value, list):
                media['video_urls'].extend([v.get('url', '') for v in value if isinstance(v, dict)])
            elif isinstance(value, str):
                media['video_urls'].append(value)
        elif col == '__website_links__':
            urls = to_array(value) or []
            media['website_urls'].extend(urls)
        else:
            row[col] = value

    # ── Submission metadata ───────────────────────────────────────────────
    row['jotform_submission_id'] = sub.get('id')
    row['submission_date']       = sub.get('created_at')
    row['last_updated']          = sub.get('updated_at')

    # ── Normalise dates ───────────────────────────────────────────────────
    row['incident_date']       = to_date(row.pop('incident_date',       None))
    row['fir_filed_date']      = to_date(row.pop('fir_filed_date',      None))
    row['cross_fir_filed_date']= to_date(row.pop('cross_fir_filed_date',None))

    # ── Normalise booleans ────────────────────────────────────────────────
    row['harassed']         = to_bool(row.pop('harassed_raw',       None))
    row['cross_fir_filed']  = to_bool(row.pop('cross_fir_filed_raw',None))
    row['online_harassment']= to_bool(row.pop('online_harassment_raw', None))
    row['displaced']        = to_bool(row.pop('displaced_raw',      None))
    row['property_damage']  = to_bool(row.pop('property_damage_raw',None))

    # ── Normalise integers ────────────────────────────────────────────────
    row['casualties'] = to_int(row.pop('casualties_raw', None))
    row['injured']    = to_int(row.pop('injured_raw',    None))

    # ── Normalise enums ───────────────────────────────────────────────────
    row['fir_status']      = normalise_fir_status(row.pop('fir_filed_raw', None))
    row['police_role']     = normalise_police_role(row.pop('police_role_raw', None))
    row['approval_status'] = normalise_approval_status(row.pop('approval_status_raw', None))

    # ── Collect FIR charges array ─────────────────────────────────────────
    fir_charges = [
        row.pop(f'fir_charges_{i}', None)
        for i in range(1, 6)
    ]
    row['fir_charges'] = [c for c in fir_charges if c] or None

    # ── Collect Cross FIR charges array ──────────────────────────────────
    cross_charges = [
        row.pop(f'cross_fir_charges_{i}', None)
        for i in range(1, 5)
    ]
    row['cross_fir_charges'] = [c for c in cross_charges if c] or None

    # ── Tags ──────────────────────────────────────────────────────────────
    if 'tags' in row:
        row['tags'] = to_array(row['tags'])

    # ── Geolocation ───────────────────────────────────────────────────────
    if 'geolocation_raw' in row:
        lat, lng = parse_geolocation(row['geolocation_raw'])
        if lat:
            row['latitude']  = lat
            row['longitude'] = lng

    # ── Extract URLs from description ────────────────────────────────────
    desc_urls = extract_urls_from_text(row.get('description', ''))
    media['website_urls'].extend(desc_urls)

    # ── Provenance — set automatically for all JotForm records ───────────
    row['data_source']          = 'jotform'
    row['verification_status']  = 'pending'
    row['reliability_level']    = '1'
    row['published']            = False
    row['sensitive']            = False

    # ── Serialise any remaining dicts/lists to JSON strings ─────────────
    for k, v in row.items():
        if isinstance(v, dict):
            row[k] = json.dumps(v)
        elif isinstance(v, list) and k not in ARRAY_COLUMNS:
            row[k] = json.dumps(v)

    # ── Keep only columns that exist in incidents table ───────────────────
    row = {k: v for k, v in row.items() if k in INCIDENT_COLUMNS and v is not None}

    # ── Wrap array columns ────────────────────────────────────────────────
    for col in ARRAY_COLUMNS:
        if col in row and not isinstance(row[col], list):
            row[col] = to_array(row[col])

    return row, media


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(DATABASE_URL)


def get_last_synced_id(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT last_submission_id FROM jotform_sync_log
               WHERE sync_type = 'incremental'
               AND new_records > 0
               ORDER BY synced_at DESC LIMIT 1"""
        )
        row = cur.fetchone()
        return row[0] if row else None


def batch_upsert_incidents(conn, rows: list[dict]) -> tuple[int, int]:
    """
    Batch upsert a list of incident rows in a single round-trip.
    Returns (new_count, updated_count).
    """
    if not rows:
        return 0, 0

    # All rows must have the same columns — use union of all keys
    all_cols = list({col for row in rows for col in row.keys()})
    # Ensure jotform_submission_id is always present
    if 'jotform_submission_id' not in all_cols:
        all_cols.append('jotform_submission_id')

    update_cols = [c for c in all_cols if c != 'jotform_submission_id']
    update_set  = ', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)
    ph          = ', '.join(['%s'] * len(all_cols))
    col_str     = ', '.join(all_cols)

    sql = f"""
        INSERT INTO incidents ({col_str})
        VALUES ({ph})
        ON CONFLICT (jotform_submission_id)
        DO UPDATE SET {update_set},
                      updated_at = NOW()
        RETURNING id, (xmax = 0) AS is_insert
    """

    # Build value tuples — fill missing cols with None
    val_tuples = [
        tuple(row.get(col) for col in all_cols)
        for row in rows
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, val_tuples, page_size=100)
        results = cur.fetchall()

    new     = sum(1 for _, is_ins in results if is_ins)
    updated = sum(1 for _, is_ins in results if not is_ins)
    return new, updated


def get_incident_ids_by_jotform(conn, jotform_ids: list[str]) -> dict[str, int]:
    """Return {jotform_submission_id: incidents.id} for a list of jotform IDs."""
    if not jotform_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            'SELECT jotform_submission_id, id FROM incidents WHERE jotform_submission_id = ANY(%s)',
            (jotform_ids,)
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def batch_upsert_media_files(conn, media_rows: list[tuple]):
    """Batch insert media files. Each tuple: (incident_id, file_type, url, platform)"""
    if not media_rows:
        return
    psycopg2.extras.execute_batch(
        conn.cursor(),
        """
        INSERT INTO media_files
            (incident_id, file_type, original_url, archived, platform, data_source)
        VALUES (%s, %s, %s, FALSE, %s, 'jotform')
        ON CONFLICT DO NOTHING
        """,
        media_rows,
        page_size=200,
    )


def batch_upsert_sources(conn, source_rows: list[tuple]):
    """Batch insert sources. Each tuple: (incident_id, url)"""
    if not source_rows:
        return
    psycopg2.extras.execute_batch(
        conn.cursor(),
        """
        INSERT INTO sources (incident_id, source_type, url, reliability)
        VALUES (%s, 'news_article', %s, 'unverified')
        ON CONFLICT DO NOTHING
        """,
        source_rows,
        page_size=200,
    )


def log_sync_run(conn, last_id, fetched, new, updated, errors, duration, sync_type='incremental'):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jotform_sync_log
                (last_submission_id, fetched, new_records, updated_records,
                 errors, duration_seconds, sync_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (last_id, fetched, new, updated, errors, duration, sync_type))
    conn.commit()


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------

def run_sync(full: bool = False):
    start = datetime.now(timezone.utc)
    log.info('=== HateWatch JotForm sync started (mode: %s) ===',
             'full' if full else 'incremental')

    conn = get_connection()

    # Determine starting point
    since_id = None if full else get_last_synced_id(conn)
    if since_id:
        log.info('Incremental sync from submission ID: %s', since_id)
    else:
        log.info('Full sync — fetching all submissions')

    # Fetch from JotForm
    submissions = fetch_submissions(since_id=since_id)
    log.info('Total submissions to process: %d', len(submissions))

    counts  = {'fetched': len(submissions), 'new': 0, 'updated': 0, 'errors': 0}
    last_id = since_id
    BATCH   = 100  # records per DB round-trip

    for batch_start in range(0, len(submissions), BATCH):
        batch = submissions[batch_start:batch_start + BATCH]
        rows, media_map = [], {}

        # ── Map all submissions in this batch ────────────────────────────
        for sub in batch:
            sub_id = sub.get('id')
            try:
                answers    = extract_answers(sub)
                row, media = map_submission(sub, answers)
                rows.append(row)
                media_map[sub_id] = media
                last_id = sub_id
            except Exception as e:
                counts['errors'] += 1
                log.error('MAP ERROR on submission %s: %s', sub_id, e)

        if not rows:
            continue

        # ── Batch upsert incidents ───────────────────────────────────────
        try:
            new, updated = batch_upsert_incidents(conn, rows)
            counts['new']     += new
            counts['updated'] += updated
            conn.commit()
        except Exception as e:
            conn.rollback()
            counts['errors'] += len(rows)
            log.error('BATCH UPSERT ERROR (offset %d): %s', batch_start, e)
            continue

        # ── Batch upsert media + sources ─────────────────────────────────
        jotform_ids = [r['jotform_submission_id'] for r in rows if 'jotform_submission_id' in r]
        id_map      = get_incident_ids_by_jotform(conn, jotform_ids)

        media_rows  = []
        source_rows = []
        for sub_id, media in media_map.items():
            inc_id = id_map.get(sub_id)
            if not inc_id:
                continue
            for url in media.get('image_urls', []):
                if url:
                    media_rows.append((inc_id, 'image', url, detect_platform(url)))
            for url in media.get('video_urls', []):
                if url:
                    media_rows.append((inc_id, 'video', url, detect_platform(url)))
            for url in media.get('website_urls', []):
                if url:
                    source_rows.append((inc_id, url))

        try:
            batch_upsert_media_files(conn, media_rows)
            batch_upsert_sources(conn, source_rows)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error('MEDIA/SOURCE BATCH ERROR (offset %d): %s', batch_start, e)

        log.info(
            'Processed batch %d–%d: +%d new, ~%d updated',
            batch_start + 1, batch_start + len(batch), new, updated
        )

    duration = (datetime.now(timezone.utc) - start).total_seconds()

    log_sync_run(
        conn, last_id,
        counts['fetched'], counts['new'], counts['updated'],
        counts['errors'], duration,
        sync_type='full' if full else 'incremental'
    )

    log.info(
        '=== Sync complete in %.1fs — fetched:%d new:%d updated:%d errors:%d ===',
        duration, counts['fetched'], counts['new'], counts['updated'], counts['errors']
    )
    conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HateWatch JotForm sync')
    parser.add_argument(
        '--full',
        action='store_true',
        help='Full re-sync of all submissions (default: incremental)'
    )
    args = parser.parse_args()
    run_sync(full=args.full)