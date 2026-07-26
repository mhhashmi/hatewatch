#!/usr/bin/env python3
"""
import_geo.py — Import India Post pincode data into geo_india table
===================================================================
Downloads and processes India Post pincode database,
then imports into the geo_india Postgres table.

Usage:
    uv run python pipeline/import_geo.py               # dry-run
    uv run python pipeline/import_geo.py --fix         # import to DB
    uv run python pipeline/import_geo.py --fix --enrich # import + enrich incidents
"""

import os
import sys
import csv
import json
import logging
import argparse
import tempfile
import urllib.request
from collections import defaultdict
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

PINCODE_URL = (
    'https://raw.githubusercontent.com/harsha1205/Pincode-India-CSV/'
    'main/India%20Pincode%20Database.csv'
)

# ---------------------------------------------------------------------------
# State normalisation
# ---------------------------------------------------------------------------
STATE_NORM = {
    'ANDAMAN AND NICOBAR ISLANDS': 'Andaman & Nicobar',
    'ANDHRA PRADESH': 'Andhra Pradesh',
    'ARUNACHAL PRADESH': 'Arunachal Pradesh',
    'ASSAM': 'Assam',
    'BIHAR': 'Bihar',
    'CHANDIGARH': 'Chandigarh',
    'CHHATTISGARH': 'Chhattisgarh',
    'DELHI': 'Delhi',
    'GOA': 'Goa',
    'GUJARAT': 'Gujarat',
    'HARYANA': 'Haryana',
    'HIMACHAL PRADESH': 'Himachal Pradesh',
    'JAMMU AND KASHMIR': 'Jammu & Kashmir',
    'JHARKHAND': 'Jharkhand',
    'KARNATAKA': 'Karnataka',
    'KERALA': 'Kerala',
    'LADAKH': 'Ladakh',
    'LAKSHADWEEP': 'Lakshadweep',
    'MADHYA PRADESH': 'Madhya Pradesh',
    'MAHARASHTRA': 'Maharashtra',
    'MANIPUR': 'Manipur',
    'MEGHALAYA': 'Meghalaya',
    'MIZORAM': 'Mizoram',
    'NAGALAND': 'Nagaland',
    'ODISHA': 'Odisha',
    'PUDUCHERRY': 'Puducherry',
    'PUNJAB': 'Punjab',
    'RAJASTHAN': 'Rajasthan',
    'SIKKIM': 'Sikkim',
    'TAMIL NADU': 'Tamil Nadu',
    'TELANGANA': 'Telangana',
    'THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU': 'Dadra & Nagar Haveli',
    'TRIPURA': 'Tripura',
    'UTTAR PRADESH': 'Uttar Pradesh',
    'UTTARAKHAND': 'Uttarakhand',
    'WEST BENGAL': 'West Bengal',
}

STATE_CODES = {
    'Andaman & Nicobar': 'AN', 'Andhra Pradesh': 'AP',
    'Arunachal Pradesh': 'AR', 'Assam': 'AS', 'Bihar': 'BR',
    'Chandigarh': 'CH', 'Chhattisgarh': 'CG', 'Delhi': 'DL',
    'Goa': 'GA', 'Gujarat': 'GJ', 'Haryana': 'HR',
    'Himachal Pradesh': 'HP', 'Jammu & Kashmir': 'JK',
    'Jharkhand': 'JH', 'Karnataka': 'KA', 'Kerala': 'KL',
    'Ladakh': 'LA', 'Lakshadweep': 'LD', 'Madhya Pradesh': 'MP',
    'Maharashtra': 'MH', 'Manipur': 'MN', 'Meghalaya': 'ML',
    'Mizoram': 'MZ', 'Nagaland': 'NL', 'Odisha': 'OD',
    'Puducherry': 'PY', 'Punjab': 'PB', 'Rajasthan': 'RJ',
    'Sikkim': 'SK', 'Tamil Nadu': 'TN', 'Telangana': 'TS',
    'Dadra & Nagar Haveli': 'DN', 'Tripura': 'TR',
    'Uttar Pradesh': 'UP', 'Uttarakhand': 'UK', 'West Bengal': 'WB',
}

UNION_TERRITORIES = {
    'Andaman & Nicobar', 'Chandigarh', 'Dadra & Nagar Haveli',
    'Delhi', 'Jammu & Kashmir', 'Ladakh', 'Lakshadweep', 'Puducherry',
}


def safe_float(val) -> Optional[float]:
    try:
        v = float(val)
        return v if v != 0.0 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Download and process
# ---------------------------------------------------------------------------

def download_pincode_data() -> list[dict]:
    log.info('Downloading India Post pincode database...')
    with urllib.request.urlopen(PINCODE_URL, timeout=30) as response:
        content = response.read().decode('utf-8')

    rows = list(csv.DictReader(content.splitlines()))
    log.info('Downloaded %d raw rows', len(rows))

    priority = {'HO': 0, 'PO': 1, 'BO': 2}
    pincode_data: dict[str, list] = defaultdict(list)

    for r in rows:
        pc = r['pincode'].strip()
        state = r['statename'].strip()
        if pc and state and state != 'NA':
            pincode_data[pc].append(r)

    final = []
    for pc, offices in pincode_data.items():
        offices.sort(key=lambda x: priority.get(x['officetype'], 3))
        best = offices[0]

        lats = [v for o in offices if (v := safe_float(o['latitude'])) is not None]
        lngs = [v for o in offices if (v := safe_float(o['longitude'])) is not None]

        state_norm = STATE_NORM.get(
            best['statename'].strip(),
            best['statename'].strip().title()
        )

        # Clamp coordinates to valid India bounds
        avg_lat = round(sum(lats)/len(lats), 6) if lats else None
        avg_lng = round(sum(lngs)/len(lngs), 6) if lngs else None
        # India bounds: lat 6-38, lng 68-98 — discard outliers
        if avg_lat and not (6 <= avg_lat <= 38):
            avg_lat = None
        if avg_lng and not (68 <= avg_lng <= 98):
            avg_lng = None

        final.append({
            'pincode':          pc,
            'post_office':      best['officename'].strip().title(),
            'division':         best['divisionname'].strip(),
            'district':         best['district'].strip().title(),
            'state':            state_norm,
            'state_code':       STATE_CODES.get(state_norm, ''),
            'union_territory':  state_norm in UNION_TERRITORIES,
            'latitude':         avg_lat,
            'longitude':        avg_lng,
            'office_count':     len(offices),
        })

    log.info('Processed %d unique pincodes', len(final))
    return final


# ---------------------------------------------------------------------------
# DB import
# ---------------------------------------------------------------------------

def import_to_db(conn, records: list[dict], dry_run: bool) -> int:
    if dry_run:
        log.info('DRY RUN — would import %d records', len(records))
        # Show sample
        for r in records[:3]:
            log.info('  Sample: %s', r)
        return 0

    log.info('Importing %d records to geo_india...', len(records))

    tuples = [
        (
            r['pincode'], r['post_office'], r['division'],
            r['district'], r['state'], r['state_code'],
            r['union_territory'], r['latitude'], r['longitude'],
            r['office_count'],
        )
        for r in records
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO geo_india (
                pincode, post_office, division,
                district, state, state_code,
                union_territory, latitude, longitude,
                office_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pincode) DO UPDATE SET
                post_office    = EXCLUDED.post_office,
                district       = EXCLUDED.district,
                state          = EXCLUDED.state,
                state_code     = EXCLUDED.state_code,
                union_territory= EXCLUDED.union_territory,
                latitude       = EXCLUDED.latitude,
                longitude      = EXCLUDED.longitude,
                office_count   = EXCLUDED.office_count,
                last_updated   = CURRENT_DATE
            """,
            tuples,
            page_size=500,
        )
    conn.commit()
    log.info('Import complete')
    return len(records)


# ---------------------------------------------------------------------------
# Enrich incidents using geo_india
# ---------------------------------------------------------------------------

def enrich_incidents(conn, dry_run: bool) -> dict:
    """
    For incidents missing state, look up from geo_india using postal_code.
    Returns counts of what was fixed.
    """
    log.info('Enriching incidents from geo_india...')

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT i.id, i.postal_code, i.city,
                   g.state, g.state_code, g.district, g.latitude, g.longitude
            FROM incidents i
            JOIN geo_india g ON g.pincode = i.postal_code
            WHERE i.state IS NULL
            AND i.retracted = FALSE
            AND i.deleted_at IS NULL
            AND i.postal_code IS NOT NULL
        """)
        pincode_matches = cur.fetchall()

    log.info('Found %d incidents to enrich via postal code', len(pincode_matches))

    if not dry_run and pincode_matches:
        updates = [
            (r['state'], r['state_code'], r['district'],
             r['latitude'], r['longitude'], r['id'])
            for r in pincode_matches
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                UPDATE incidents SET
                    state      = %s,
                    state_code = %s,
                    district   = COALESCE(district, %s),
                    latitude   = COALESCE(latitude, %s),
                    longitude  = COALESCE(longitude, %s),
                    updated_at = NOW()
                WHERE id = %s
                """,
                updates,
                page_size=200,
            )
        conn.commit()

    return {'pincode_enriched': len(pincode_matches)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool, enrich: bool):
    mode = 'DRY RUN' if dry_run else 'LIVE'
    log.info('=== import_geo.py started (%s) ===', mode)

    conn    = psycopg2.connect(DATABASE_URL)
    records = download_pincode_data()

    imported = import_to_db(conn, records, dry_run)

    print()
    print('=' * 55)
    print(f'  Geo Import Report ({mode})')
    print('=' * 55)
    print(f'  Pincodes processed:  {len(records):,}')
    print(f'  {"Would import" if dry_run else "Imported to DB"}:  {len(records):,}')

    states = sorted(set(r['state'] for r in records))
    print(f'  States/UTs covered:  {len(states)}')
    uts = [r for r in records if r['union_territory']]
    print(f'  Union territories:   {len(set(r["state"] for r in uts))}')
    print()

    if enrich and not dry_run:
        counts = enrich_incidents(conn, dry_run)
        print(f'  Incidents enriched via pincode: {counts["pincode_enriched"]}')

    print('=' * 55)
    if dry_run:
        print('  Run with --fix to import to DB')
        print('  Run with --fix --enrich to also update incidents')
    print()

    conn.close()
    log.info('=== import_geo.py complete ===')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Import India Post pincode data into geo_india table'
    )
    parser.add_argument('--fix', action='store_true',
                        help='Import to DB (default: dry-run)')
    parser.add_argument('--enrich', action='store_true',
                        help='Also enrich incidents table from geo_india')
    args = parser.parse_args()
    run(dry_run=not args.fix, enrich=args.enrich)