#!/usr/bin/env python3
"""
query.py — HateWatch data explorer
====================================
Filter, view, and export incident records from the database.

Usage:
    uv run python pipeline/query.py                              # show all (paginated)
    uv run python pipeline/query.py --state "Uttar Pradesh"
    uv run python pipeline/query.py --type lynching,murder
    uv run python pipeline/query.py --city Amroha
    uv run python pipeline/query.py --from 2024-01-01 --to 2024-12-31
    uv run python pipeline/query.py --verified ai_verified
    uv run python pipeline/query.py --confidence high
    uv run python pipeline/query.py --search "love jihad"
    uv run python pipeline/query.py --stats
    uv run python pipeline/query.py --detail --limit 5
    uv run python pipeline/query.py --format csv --out reports/results.csv
    uv run python pipeline/query.py --format json --out reports/results.json
    uv run python pipeline/query.py --ids-only                  # pipe into ai_verify
"""

import os
import sys
import csv
import json
import logging
import argparse
import textwrap
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.WARNING,  # quiet by default — output is the data
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise SystemExit('ERROR: DATABASE_URL missing from .env')

# ---------------------------------------------------------------------------
# Valid incident types for --type flag
# ---------------------------------------------------------------------------
VALID_TYPES = [
    'lynching', 'mob_assault', 'murder', 'rape', 'sexual_assault',
    'property_destruction', 'arson', 'demolition', 'eviction',
    'false_fir', 'uapa_detention', 'nsa_detention', 'hate_speech',
    'economic_boycott', 'forced_conversion_allegation',
    'religious_site_destruction', 'cow_vigilantism',
    'anti_conversion_law_misuse', 'love_jihad_allegation',
    'communal_riot', 'targeted_arrest', 'other',
]

VALID_VERIFIED = [
    'ai_verified', 'jotform_approved', 'verified',
    'needs_sources', 'in_review', 'none', 'published', 'retracted',
]

VALID_CONFIDENCE = ['high', 'medium', 'low', 'none']

# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

def build_query(args) -> tuple[str, list]:
    """Build SQL query from filter args. Returns (sql, params)."""

    where  = ['i.deleted_at IS NULL']
    params = []

    # Default: exclude retracted unless explicitly requested
    if not getattr(args, 'include_retracted', False):
        where.append('i.retracted = FALSE')

    # --- Incident filters ---
    if args.state:
        where.append('i.state ILIKE %s')
        params.append(f'%{args.state}%')

    if args.city:
        where.append('i.city ILIKE %s')
        params.append(f'%{args.city}%')

    if args.district:
        where.append('i.district ILIKE %s')
        params.append(f'%{args.district}%')

    if args.type:
        types = [t.strip() for t in args.type.split(',')]
        # Match against both incident_type enum and nature_of_incident text
        type_conditions = []
        for t in types:
            type_conditions.append('i.incident_type::text ILIKE %s')
            params.append(f'%{t}%')
            type_conditions.append('i.nature_of_incident ILIKE %s')
            params.append(f'%{t}%')
        where.append(f'({" OR ".join(type_conditions)})')

    if args.from_date:
        where.append('i.incident_date >= %s')
        params.append(args.from_date)

    if args.to_date:
        where.append('i.incident_date <= %s')
        params.append(args.to_date)

    if args.search:
        where.append('(i.description ILIKE %s OR i.nature_of_incident ILIKE %s)')
        params.extend([f'%{args.search}%', f'%{args.search}%'])

    if args.person:
        # Search raw JSON blobs, text fields, AND persons table
        where.append(
            '('
            "  i.suspect_details_raw ILIKE %s"
            "  OR i.victim_details_raw ILIKE %s"
            "  OR i.description ILIKE %s"
            "  OR i.hate_speech_made_by ILIKE %s"
            "  OR i.fir_filed_against ILIKE %s"
            '  OR i.id IN ('
            '    SELECT ip.incident_id FROM incident_persons ip'
            '    JOIN persons p ON ip.person_id = p.id'
            '    WHERE p.full_name ILIKE %s OR p.name_original ILIKE %s'
            '  )'
            ')'
        )
        params.extend([f'%{args.person}%'] * 7)

    if args.party:
        # Search party in state_government_party field AND text fields
        where.append(
            '('
            '  i.state_government_party ILIKE %s'
            '  OR i.description ILIKE %s'
            '  OR i.suspect_details_raw ILIKE %s'
            '  OR i.victim_details_raw ILIKE %s'
            '  OR i.id IN ('
            '    SELECT ip.incident_id FROM incident_persons ip'
            '    JOIN persons p ON ip.person_id = p.id'
            '    WHERE p.political_affiliation ILIKE %s'
            '  )'
            ')'
        )
        params.extend([f'%{args.party}%'] * 5)

    if args.org:
        # Search org name in raw JSON, text fields AND organisations table
        where.append(
            '('
            "  i.suspect_details_raw ILIKE %s"
            "  OR i.victim_details_raw ILIKE %s"
            "  OR i.description ILIKE %s"
            "  OR i.hate_speech_made_by ILIKE %s"
            '  OR i.id IN ('
            '    SELECT ip.incident_id FROM incident_persons ip'
            '    JOIN persons p ON ip.person_id = p.id'
            '    JOIN organisations o ON p.org_id = o.id'
            '    WHERE o.name ILIKE %s'
            '  )'
            ')'
        )
        params.extend([f'%{args.org}%'] * 5)

    if args.keyword:
        where.append(
            '('
            '  i.description ILIKE %s'
            '  OR i.nature_of_incident ILIKE %s'
            '  OR i.hate_speech_made_by ILIKE %s'
            '  OR i.fir_filed_against ILIKE %s'
            '  OR i.suspect_details_raw ILIKE %s'
            '  OR i.victim_details_raw ILIKE %s'
            ')'
        )
        params.extend([f'%{args.keyword}%'] * 6)

    if args.fir:
        where.append('i.fir_status = %s')
        params.append(args.fir)

    if args.severity:
        where.append('i.incident_severity::text ILIKE %s')
        params.append(f'%{args.severity}%')

    # --- Verification filters ---
    if args.verified:
        v = args.verified.lower()
        if v == 'ai_verified':
            where.append("""i.id IN (
                SELECT DISTINCT incident_id FROM ai_costs
                WHERE script = 'ai_verify' AND incident_id IS NOT NULL
            )""")
        elif v == 'jotform_approved':
            where.append("i.approval_status = 'approved'")
        elif v == 'none':
            where.append("i.reliability_level = '1'")
            where.append("""i.id NOT IN (
                SELECT DISTINCT incident_id FROM ai_costs
                WHERE script = 'ai_verify' AND incident_id IS NOT NULL
            )""")
        elif v == 'needs_sources':
            where.append("i.verification_status = 'needs_sources'")
        elif v == 'in_review':
            where.append("i.verification_status = 'in_review'")
        elif v == 'verified':
            where.append("i.verification_status = 'verified'")
        elif v == 'published':
            where.append("i.published = TRUE")
        elif v == 'retracted':
            where.append("i.retracted = TRUE")

    if args.confidence:
        c = args.confidence.lower()
        if c == 'high':
            where.append("""i.id IN (
                SELECT DISTINCT incident_id FROM ai_staging
                WHERE confidence_score >= 0.8 AND incident_id IS NOT NULL
            )""")
        elif c == 'medium':
            where.append("""i.id IN (
                SELECT DISTINCT incident_id FROM ai_staging
                WHERE confidence_score >= 0.5 AND confidence_score < 0.8
                AND incident_id IS NOT NULL
            )""")
        elif c == 'low':
            where.append("""i.id IN (
                SELECT DISTINCT incident_id FROM ai_staging
                WHERE confidence_score < 0.5 AND incident_id IS NOT NULL
            )""")
        elif c == 'none':
            where.append("""i.id NOT IN (
                SELECT DISTINCT incident_id FROM ai_staging
                WHERE incident_id IS NOT NULL
            )""")

    # --- Specific IDs ---
    if getattr(args, 'ids', None):
        id_list = [int(x.strip()) for x in args.ids.split(',')]
        placeholders = ','.join(['%s'] * len(id_list))
        where.append(f'i.id IN ({placeholders})')
        params.extend(id_list)

    where_sql = 'WHERE ' + '\nAND '.join(where)

    sql = f"""
        SELECT
            i.*,
            -- AI verification info
            s.confidence_score,
            s.extraction_notes as ai_notes
        FROM incidents i
        LEFT JOIN LATERAL (
            SELECT s2.confidence_score, s2.extraction_notes
            FROM ai_staging s2
            JOIN ai_costs ac ON ac.incident_id = i.id
            WHERE s2.confidence_score IS NOT NULL
            AND ac.script = 'ai_verify'
            ORDER BY s2.created_at DESC
            LIMIT 1
        ) s ON TRUE
        {where_sql}
        ORDER BY i.incident_date DESC NULLS LAST, i.id DESC
    """

    return sql, params


def fetch_records(conn, sql: str, params: list, limit: int, offset: int = 0) -> list[dict]:
    paginated = sql + f'\nLIMIT {limit} OFFSET {offset}'
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(paginated, params)
        return [dict(r) for r in cur.fetchall()]


def count_records(conn, sql: str, params: list) -> int:
    count_sql = f'SELECT COUNT(*) FROM ({sql}) sub'
    with conn.cursor() as cur:
        cur.execute(count_sql, params)
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def show_stats(conn, sql: str, params: list):
    """Show aggregate statistics for the filtered recordset."""

    base = f'SELECT * FROM ({sql}) sub'

    def query(agg_sql):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(agg_sql, params * 2 if 'sub2' in agg_sql else params)
            return cur.fetchall()

    total = count_records(conn, sql, params)

    # By state
    by_state = query(f"""
        SELECT state, COUNT(*) as count
        FROM ({sql}) sub
        WHERE state IS NOT NULL
        GROUP BY state ORDER BY count DESC LIMIT 10
    """)

    # By type
    by_type = query(f"""
        SELECT
            COALESCE(incident_type::text, nature_of_incident, 'unknown') as itype,
            COUNT(*) as count
        FROM ({sql}) sub
        GROUP BY itype ORDER BY count DESC LIMIT 10
    """)

    # By year
    by_year = query(f"""
        SELECT
            EXTRACT(YEAR FROM incident_date)::int as yr,
            COUNT(*) as count
        FROM ({sql}) sub
        WHERE incident_date IS NOT NULL
        GROUP BY yr ORDER BY yr DESC LIMIT 8
    """)

    # By verification
    by_status = query(f"""
        SELECT verification_status, COUNT(*) as count
        FROM ({sql}) sub
        GROUP BY verification_status ORDER BY count DESC
    """)

    print()
    print('=' * 55)
    print(f'  HateWatch — Query Statistics')
    print(f'  Total matching records: {total:,}')
    print('=' * 55)

    print()
    print('  By state (top 10):')
    for r in by_state:
        bar = '█' * min(int(r['count'] / max(total / 30, 1)), 30)
        print(f'    {str(r["state"]):<25} {r["count"]:>5,}  {bar}')

    print()
    print('  By incident type (top 10):')
    for r in by_type:
        print(f'    {str(r["itype"]):<35} {r["count"]:>5,}')

    print()
    print('  By year:')
    for r in by_year:
        if r['yr']:
            bar = '█' * min(int(r['count'] / max(total / 30, 1)), 30)
            print(f'    {r["yr"]}  {r["count"]:>5,}  {bar}')

    print()
    print('  By verification status:')
    for r in by_status:
        print(f'    {str(r["verification_status"]):<25} {r["count"]:>5,}')

    print()
    print('=' * 55)
    print()


# ---------------------------------------------------------------------------
# Display formats
# ---------------------------------------------------------------------------

def fmt_date(d) -> str:
    return str(d)[:10] if d else '—'


def fmt_str(s, width=0) -> str:
    if not s:
        return '—'
    s = str(s)
    if width and len(s) > width:
        return s[:width-1] + '…'
    return s


def print_table(records: list[dict], total: int, offset: int):
    """Print compact summary table."""
    if not records:
        print('  No records found.')
        return

    # Header
    print()
    print(f'  {"ID":<7} {"Date":<12} {"City":<18} {"State":<18} '
          f'{"Type":<22} {"Conf":>5}  {"Status":<15}  Description')
    print(f'  {"─"*7} {"─"*12} {"─"*18} {"─"*18} '
          f'{"─"*22} {"─"*5}  {"─"*15}  {"─"*35}')

    for r in records:
        itype = (r.get('incident_type') or r.get('nature_of_incident') or '—')
        conf  = f'{float(r["confidence_score"]):.0%}' if r.get('confidence_score') else '—'
        desc  = (r.get('description') or '—')[:40].replace('\n', ' ')

        print(f'  {r["id"]:<7} {fmt_date(r["incident_date"]):<12} '
              f'{fmt_str(r["city"], 18):<18} {fmt_str(r["state"], 18):<18} '
              f'{fmt_str(itype, 22):<22} {conf:>5}  '
              f'{fmt_str(r["verification_status"], 15):<15}  {desc}')

    showing_to = min(offset + len(records), total)
    print()
    print(f'  Showing {offset+1}–{showing_to} of {total:,} records')


def fetch_related(conn, incident_id: int) -> dict:
    """Fetch all related records for an incident."""
    related = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Sources
        cur.execute("""
            SELECT source_type, title, url, archived_url, reliability, publication_date
            FROM sources WHERE incident_id = %s ORDER BY publication_date DESC
        """, (incident_id,))
        related['sources'] = [dict(r) for r in cur.fetchall()]

        # Media files
        cur.execute("""
            SELECT file_type, original_url, hetzner_path, archived, platform
            FROM media_files WHERE incident_id = %s ORDER BY file_type
        """, (incident_id,))
        related['media'] = [dict(r) for r in cur.fetchall()]

        # AI verification results
        cur.execute("""
            SELECT confidence_score, extraction_notes, review_status,
                   ai_model, created_at
            FROM ai_staging
            WHERE raw_json::jsonb IS NOT NULL
            ORDER BY created_at DESC LIMIT 3
        """)
        related['ai_results'] = [dict(r) for r in cur.fetchall()]

        # Persons linked to incident
        cur.execute("""
            SELECT p.full_name, p.person_type, ip.role, ip.direct_responsibility
            FROM incident_persons ip
            JOIN persons p ON ip.person_id = p.id
            WHERE ip.incident_id = %s
        """, (incident_id,))
        related['persons'] = [dict(r) for r in cur.fetchall()]

        # Legal actions
        cur.execute("""
            SELECT action_type, action_date, court_name, charges, outcome, current_status
            FROM legal_actions WHERE incident_id = %s ORDER BY action_date
        """, (incident_id,))
        related['legal'] = [dict(r) for r in cur.fetchall()]

    return related


def print_detail(records: list[dict], total: int, conn=None):
    """Print full detail for each record including related data."""
    for r in records:
        itype = r.get('incident_type') or r.get('nature_of_incident') or '—'
        conf  = f'{float(r["confidence_score"]):.0%}' if r.get('confidence_score') else 'not verified'

        print()
        print('═' * 65)
        print(f'  INCIDENT #{r["id"]}')
        print('═' * 65)

        # Core fields
        print(f'  Date:              {fmt_date(r["incident_date"])}')
        print(f'  Type:              {itype}')
        print(f'  Severity:          {r.get("incident_severity") or "—"}')
        print(f'  Bias motivation:   {r.get("bias_motivation") or "—"}')
        print()

        # Location
        print(f'  City:              {r.get("city") or "—"}')
        print(f'  District:          {r.get("district") or "—"}')
        print(f'  State:             {r.get("state") or "—"}')
        print(f'  Postal code:       {r.get("postal_code") or "—"}')
        lat = r.get("latitude")
        lng = r.get("longitude")
        if lat and lng:
            print(f'  Coordinates:       {lat}, {lng}')
        print()

        # Victim / perpetrator
        print(f'  Casualties:        {r.get("casualties") or 0}')
        print(f'  Injured:           {r.get("injured") or 0}')
        print(f'  Harassed:          {r.get("harassed") or "—"}')
        print(f'  Displaced:         {r.get("displaced") or "—"}')
        print(f'  No. of suspects:   {r.get("number_of_suspects") or "—"}')
        print(f'  Hate speech by:    {r.get("hate_speech_made_by") or "—"}')
        print()

        # Legal
        print(f'  FIR status:        {r.get("fir_status") or "—"}')
        print(f'  FIR filed date:    {fmt_date(r.get("fir_filed_date"))}')
        print(f'  FIR charges:       {r.get("fir_charges") or "—"}')
        print(f'  FIR filed against: {r.get("fir_filed_against") or "—"}')
        print(f'  Police station:    {r.get("police_station") or "—"}')
        print(f'  Police role:       {r.get("police_role") or "—"}')
        print(f'  Investigating:     {r.get("investigating_agency") or "—"}')
        print(f'  Case status:       {r.get("case_current_status") or "—"}')
        if r.get("cross_fir_filed"):
            print(f'  Cross FIR:         Yes — against {r.get("cross_fir_filed_against") or "unknown"}')
        print()

        # Political context
        print(f'  State govt party:  {r.get("state_government_party") or "—"}')
        print(f'  Tags:              {r.get("tags") or "—"}')
        print()

        # Description
        if r.get('description'):
            print('  DESCRIPTION:')
            for line in textwrap.wrap(r['description'], width=62):
                print(f'    {line}')
        print()

        # Victim/suspect raw
        if r.get('victim_details_raw'):
            print(f'  Victim details:    {str(r["victim_details_raw"])[:200]}')
        if r.get('suspect_details_raw'):
            print(f'  Suspect details:   {str(r["suspect_details_raw"])[:200]}')
        print()

        # Verification
        print('  VERIFICATION:')
        print(f'    JotForm approval:   {r.get("approval_status") or "—"}')
        print(f'    Verification status:{r.get("verification_status") or "—"}')
        print(f'    Reliability level:  {r.get("reliability_level") or "—"}')
        print(f'    AI confidence:      {conf}')
        print(f'    Published:          {r.get("published") or False}')
        print(f'    Sensitive:          {r.get("sensitive") or False}')
        if r.get('review_notes'):
            print()
            print('  AI REVIEW NOTES:')
            for line in textwrap.wrap(r['review_notes'], width=62):
                print(f'    {line}')
        print()

        # Related records from other tables
        if conn:
            related = fetch_related(conn, r['id'])

            if related['sources']:
                print(f'  SOURCES ({len(related["sources"])}):')
                for s in related['sources']:
                    print(f'    [{s["source_type"]}] {s.get("title") or s.get("url") or "—"}')
                    if s.get('url'):
                        print(f'      URL: {s["url"]}')
                print()

            if related['media']:
                print(f'  MEDIA FILES ({len(related["media"])}):')
                for m in related['media']:
                    archived = '✓ archived' if m.get('archived') else '⏳ pending'
                    print(f'    [{m["file_type"]}] {m.get("platform") or "—"} | {archived}')
                    print(f'      {m.get("original_url") or "—"}')
                print()

            if related['persons']:
                print(f'  PERSONS ({len(related["persons"])}):')
                for p in related['persons']:
                    direct = ' (direct responsibility)' if p.get('direct_responsibility') else ''
                    print(f'    {p["full_name"]} — {p["role"]}{direct}')
                print()

            if related['legal']:
                print(f'  LEGAL ACTIONS ({len(related["legal"])}):')
                for l in related['legal']:
                    print(f'    [{l["action_type"]}] {fmt_date(l.get("action_date"))} | {l.get("outcome") or l.get("current_status") or "pending"}')
                print()

        # Metadata
        print(f'  JotForm ID:  {r.get("jotform_submission_id") or "—"}')
        print(f'  Created:     {fmt_date(r.get("created_at"))}')
        print(f'  Updated:     {fmt_date(r.get("updated_at"))}')

    print()
    print('═' * 65)
    print(f'  Total: {total:,} records matching filters')


def export_csv(records: list[dict], out_path: str):
    """Export records to CSV."""
    if not records:
        print('  No records to export.')
        return

    os.makedirs(os.path.dirname(out_path) if os.path.dirname(out_path) else '.', exist_ok=True)

    # Flatten for CSV
    flat_keys = [
        'id', 'incident_date', 'incident_type', 'nature_of_incident',
        'city', 'district', 'state', 'casualties', 'injured',
        'fir_status', 'police_role', 'approval_status',
        'verification_status', 'reliability_level', 'confidence_score',
        'published', 'review_notes', 'description', 'jotform_submission_id',
    ]

    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_keys, extrasaction='ignore')
        writer.writeheader()
        for r in records:
            row = {k: (str(v).replace('\n', ' ') if v is not None else '') for k, v in r.items()}
            writer.writerow(row)

    print(f'  Exported {len(records):,} records to: {out_path}')


def export_json(records: list[dict], out_path: str):
    """Export records to JSON."""
    if not records:
        print('  No records to export.')
        return

    os.makedirs(os.path.dirname(out_path) if os.path.dirname(out_path) else '.', exist_ok=True)

    # Make JSON serialisable
    clean = []
    for r in records:
        row = {}
        for k, v in r.items():
            if hasattr(v, 'isoformat'):
                row[k] = v.isoformat()
            elif v is None:
                row[k] = None
            else:
                row[k] = str(v) if not isinstance(v, (int, float, bool)) else v
        clean.append(row)

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)

    print(f'  Exported {len(records):,} records to: {out_path}')


def print_ids_only(records: list[dict]):
    """Print just the IDs — for piping into ai_verify."""
    for r in records:
        print(r['id'])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(args):
    conn = psycopg2.connect(DATABASE_URL)
    sql, params = build_query(args)

    # Stats mode
    if args.stats:
        show_stats(conn, sql, params)
        conn.close()
        return

    # Count total
    total = count_records(conn, sql, params)

    if total == 0:
        print()
        print('  No records found matching your filters.')
        print()
        conn.close()
        return

    # Determine limit
    limit  = args.limit if args.limit else (total if args.format != 'table' else 50)
    offset = args.offset if args.offset else 0

    records = fetch_records(conn, sql, params, limit, offset)

    # IDs only — for piping
    if args.ids_only:
        print_ids_only(records)
        conn.close()
        return

    # Print filter summary
    if args.format == 'table':
        print()
        filters = []
        if args.state:      filters.append(f'state={args.state}')
        if args.city:       filters.append(f'city={args.city}')
        if args.type:       filters.append(f'type={args.type}')
        if args.from_date:  filters.append(f'from={args.from_date}')
        if args.to_date:    filters.append(f'to={args.to_date}')
        if args.verified:   filters.append(f'verified={args.verified}')
        if args.confidence: filters.append(f'confidence={args.confidence}')
        if args.search:     filters.append(f'search="{args.search}"')
        if args.person:     filters.append(f'person="{args.person}"')
        if args.party:      filters.append(f'party="{args.party}"')
        if args.org:        filters.append(f'org="{args.org}"')
        if args.keyword:    filters.append(f'keyword="{args.keyword}"')
        if filters:
            print(f'  Filters: {" | ".join(filters)}')
        print(f'  Found: {total:,} records')

    # Output
    if args.format == 'csv':
        out = args.out or f'/tmp/hatewatch_{_timestamp()}.csv'
        all_records = fetch_records(conn, sql, params, total, 0)
        # Show table on screen
        if args.detail:
            print_detail(all_records, total, conn)
        else:
            print_table(all_records, total, 0)
        # Save to file
        export_csv(all_records, out)

    elif args.format == 'json':
        out = args.out or f'/tmp/hatewatch_{_timestamp()}.json'
        all_records = fetch_records(conn, sql, params, total, 0)
        # Show JSON on screen
        print()
        print(json.dumps(
            [{k: (v.isoformat() if hasattr(v, 'isoformat') else v)
              for k, v in r.items()} for r in all_records],
            indent=2, ensure_ascii=False
        ))
        # Save to file
        export_json(all_records, out)

    elif args.detail:
        print_detail(records, total, conn)

    else:
        print_table(records, total, offset)
        if total > limit:
            print(f'  Use --limit {total} to see all, or --offset N to page')
            print(f'  Export: --format csv --out reports/results.csv')
        print()

    conn.close()


def _timestamp() -> str:
    from datetime import datetime
    return datetime.now().strftime('%Y%m%d_%H%M%S')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='HateWatch data explorer — filter, view, and export incidents',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  query.py --state "Uttar Pradesh" --type lynching
  query.py --from 2024-01-01 --to 2024-12-31 --stats
  query.py --verified needs_sources --detail
  query.py --search "Yogi Adityanath" --format json --out reports/yogi.json
  query.py --person "Yogi Adityanath" --detail
  query.py --party BJP --state "Uttar Pradesh" --stats
  query.py --org "Bajrang Dal" --type lynching
  query.py --keyword "bulldozer" --from 2022-01-01
  query.py --state UP --ids-only | xargs -I{} echo {}
        """
    )

    # Incident filters
    g = parser.add_argument_group('Incident filters')
    g.add_argument('--state',     help='Filter by state (partial match)')
    g.add_argument('--city',      help='Filter by city (partial match)')
    g.add_argument('--district',  help='Filter by district (partial match)')
    g.add_argument('--type',      help='Incident type(s), comma-separated '
                                       '(e.g. lynching,murder,hate_speech)')
    g.add_argument('--severity',  help='Incident severity (e.g. death, assault)')
    g.add_argument('--from',      dest='from_date', metavar='DATE',
                   help='From date (YYYY-MM-DD)')
    g.add_argument('--to',        dest='to_date',   metavar='DATE',
                   help='To date (YYYY-MM-DD)')
    g.add_argument('--fir',       help='FIR status: filed, not_filed, unknown')
    g.add_argument('--search',    help='Keyword search in description')
    g.add_argument('--ids',       help='Specific IDs, comma-separated (e.g. 1,2,3)')
    g.add_argument('--person',    help='Search by person name (victim, perpetrator, official)')
    g.add_argument('--party',     help='Search by political party (e.g. BJP, Congress)')
    g.add_argument('--org',       help='Search by organisation name (e.g. VHP, Bajrang Dal)')
    g.add_argument('--keyword',   help='Broad keyword search across all text fields')

    # Verification filters
    v = parser.add_argument_group('Verification filters')
    v.add_argument('--verified',    choices=VALID_VERIFIED,
                   help='Verification status filter')
    v.add_argument('--confidence',  choices=VALID_CONFIDENCE,
                   help='AI confidence level: high (80pct+), medium (50-80pct), low (<50pct), none')

    # Output options
    o = parser.add_argument_group('Output options')
    o.add_argument('--format', '-f', choices=['table', 'csv', 'json'],
                   default='table', help='Output format (default: table)')
    o.add_argument('--detail',    action='store_true',
                   help='Show full record detail')
    o.add_argument('--stats',     action='store_true',
                   help='Show aggregate statistics instead of records')
    o.add_argument('--ids-only',  action='store_true', dest='ids_only',
                   help='Output IDs only (for piping into ai_verify)')
    o.add_argument('--limit',     type=int, default=None,
                   help='Max records to show (default: 50 for table, all for export)')
    o.add_argument('--offset',    type=int, default=0,
                   help='Pagination offset (default: 0)')
    o.add_argument('--out', '-o', help='Output file path (default: /tmp/hatewatch_TIMESTAMP.csv/json)')

    args = parser.parse_args()
    run(args)