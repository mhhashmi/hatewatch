#!/usr/bin/env python3
"""
audit.py — HateWatch data quality & improvement tracker
========================================================
Compares raw_incidents (exact source data) vs incidents (enriched)
to show exactly what changed, when, and by what method.

All data comes from raw_incidents JOIN incidents — no extra logging needed.

Usage:
    uv run python pipeline/audit.py --coverage          # field completeness stats
    uv run python pipeline/audit.py --compare           # side-by-side raw vs enriched
    uv run python pipeline/audit.py --compare --limit 20
    uv run python pipeline/audit.py --id 41614          # full audit for one incident
    uv run python pipeline/audit.py --conflicts         # where AI and rules disagree
    uv run python pipeline/audit.py --stats             # aggregate improvement metrics
    uv run python pipeline/audit.py --compare -f csv -o /tmp/audit.csv
    uv run python pipeline/audit.py --compare -f json -o /tmp/audit.json
"""

import os
import sys
import csv
import json
import logging
import argparse
import textwrap
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)],
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise SystemExit('ERROR: DATABASE_URL missing from .env')

# ---------------------------------------------------------------------------
# Fields to audit — raw column → enriched column
# ---------------------------------------------------------------------------
AUDIT_FIELDS = [
    # (display_name, raw_col, enriched_col, source_of_enrichment)
    ('incident_type',    'nature_of_incident',     'incident_type',             'rule+ai'),
    ('severity',         'hate_crime_included',     'incident_severity',         'rule+ai'),
    ('bias_motivation',  None,                      'bias_motivation',           'ai'),
    ('victim_religion',  'victim_details_raw',      'victim_religion',           'rule+ai'),
    ('victim_caste',     'victim_details_raw',      'victim_caste',              'rule'),
    ('suspect_org',      'suspect_details_raw',     'suspect_org_affiliation',   'rule+ai'),
    ('suspect_religion', 'suspect_details_raw',     'suspect_religion',          'rule'),
    ('perpetrator_type', None,                      'perpetrator_type',          'ai'),
    ('motive_cause',     'motive_cause',            'motive_cause',              'jotform'),
    ('title',            'title',                   'title',                     'jotform'),
    ('fir_status',       'fir_filed_raw',           'fir_status',                'rule'),
    ('police_role',      'police_role_raw',         'police_role',               'rule'),
    ('state',            'address_raw',             'state',                     'rule'),
    ('jotform_approval', 'jotform_approval',        'jotform_approval',          'csv'),
]


def fmt(val, width=30) -> str:
    if val is None:
        return '—'
    if isinstance(val, list):
        val = ', '.join(str(v) for v in val)
    s = str(val).replace('\n', ' ').strip()
    if width and len(s) > width:
        return s[:width-1] + '…'
    return s


def get_conn():
    return psycopg2.connect(DATABASE_URL)


# ---------------------------------------------------------------------------
# --coverage: field completeness stats
# ---------------------------------------------------------------------------

def show_coverage(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT COUNT(*) as total FROM incidents
            WHERE retracted = FALSE AND deleted_at IS NULL
        """)
        total = cur.fetchone()['total']

        cur.execute("""
            SELECT
                COUNT(*)                        as total,
                COUNT(incident_type)            as incident_type,
                COUNT(incident_severity)        as severity,
                COUNT(bias_motivation)          as bias_motivation,
                COUNT(victim_religion)          as victim_religion,
                COUNT(victim_caste)             as victim_caste,
                COUNT(suspect_org_affiliation)  as suspect_org,
                COUNT(suspect_religion)         as suspect_religion,
                COUNT(perpetrator_type)         as perpetrator_type,
                COUNT(motive_cause)             as motive_cause,
                COUNT(title)                    as title,
                COUNT(state)                    as state,
                COUNT(city)                     as city,
                COUNT(fir_status)               as fir_status,
                COUNT(jotform_approval)         as jotform_approval,
                COUNT(verification_log)         as has_verification_log
            FROM incidents
            WHERE retracted = FALSE AND deleted_at IS NULL
        """)
        counts = dict(cur.fetchone())

        # Also get raw coverage for comparison
        cur.execute("""
            SELECT
                COUNT(*)                as total,
                COUNT(nature_of_incident) as nature,
                COUNT(victim_details_raw) as victim_raw,
                COUNT(suspect_details_raw) as suspect_raw,
                COUNT(motive_cause)     as motive_cause,
                COUNT(jotform_approval) as jotform_approval
            FROM raw_incidents
        """)
        raw_counts = dict(cur.fetchone())

    print()
    print('=' * 70)
    print('  HateWatch — Field Coverage Report')
    print(f'  Total active incidents: {total:,}')
    print('=' * 70)
    print()
    print(f'  {"Field":<25} {"Enriched":>10} {"Coverage":>10}  {"Source":<12}  Bar')
    print(f'  {"─"*25} {"─"*10} {"─"*10}  {"─"*12}  {"─"*25}')

    fields = [
        ('incident_type',    'incident_type',    'rule+ai'),
        ('incident_severity','severity',          'rule+ai'),
        ('bias_motivation',  'bias_motivation',   'ai only'),
        ('victim_religion',  'victim_religion',   'rule+ai'),
        ('victim_caste',     'victim_caste',      'rule'),
        ('suspect_org',      'suspect_org',       'rule+ai'),
        ('suspect_religion', 'suspect_religion',  'rule'),
        ('perpetrator_type', 'perpetrator_type',  'ai only'),
        ('motive_cause',     'motive_cause',      'jotform'),
        ('title',            'title',             'jotform'),
        ('state',            'state',             'rule+geo'),
        ('city',             'city',              'jotform'),
        ('fir_status',       'fir_status',        'rule'),
        ('jotform_approval', 'jotform_approval',  'csv import'),
    ]

    for display, key, source in fields:
        count = counts.get(key, 0)
        pct   = count / total * 100 if total else 0
        bar   = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
        print(f'  {display:<25} {count:>10,} {pct:>9.1f}%  {source:<12}  {bar}')

    print()
    print('  Raw data coverage (for comparison):')
    print(f'  {"Field":<25} {"Raw count":>10}')
    print(f'  {"─"*25} {"─"*10}')
    raw_fields = [
        ('nature_of_incident', 'nature'),
        ('victim_details_raw', 'victim_raw'),
        ('suspect_details_raw','suspect_raw'),
        ('motive_cause',       'motive_cause'),
        ('jotform_approval',   'jotform_approval'),
    ]
    for display, key in raw_fields:
        count = raw_counts.get(key, 0)
        rtotal = raw_counts.get('total', 1)
        pct = count / rtotal * 100 if rtotal else 0
        print(f'  {display:<25} {count:>10,}  ({pct:.1f}%)')

    print()
    print('=' * 70)
    print()


# ---------------------------------------------------------------------------
# --compare: side-by-side raw vs enriched
# ---------------------------------------------------------------------------

def fetch_compare_records(conn, limit: int, offset: int,
                          ai_only: bool) -> list[dict]:
    ai_filter = """
        AND i.id IN (
            SELECT DISTINCT incident_id FROM ai_costs
            WHERE script = 'process_ai' AND incident_id IS NOT NULL
        )
    """ if ai_only else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                i.id,
                i.incident_date,
                i.city,
                i.state,
                -- raw fields
                r.nature_of_incident        AS raw_nature,
                r.hate_crime_included       AS raw_hate_crime,
                r.victim_details_raw        AS raw_victim,
                r.suspect_details_raw       AS raw_suspect,
                r.motive_cause              AS raw_motive,
                r.fir_filed_raw             AS raw_fir,
                r.police_role_raw           AS raw_police_role,
                r.jotform_approval          AS raw_jotform_approval,
                -- enriched fields
                i.incident_type,
                i.incident_severity,
                i.bias_motivation,
                i.victim_religion,
                i.victim_caste,
                i.suspect_org_affiliation,
                i.suspect_religion,
                i.perpetrator_type,
                i.motive_cause,
                i.fir_status,
                i.police_role,
                i.jotform_approval          AS enriched_jotform_approval,
                -- verification log
                i.verification_log
            FROM incidents i
            JOIN raw_incidents r
              ON r.jotform_submission_id = i.jotform_submission_id
            WHERE i.retracted = FALSE
            AND i.deleted_at IS NULL
            {ai_filter}
            ORDER BY i.id DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        return [dict(r) for r in cur.fetchall()]


def count_compare_records(conn, ai_only: bool) -> int:
    ai_filter = """
        AND i.id IN (
            SELECT DISTINCT incident_id FROM ai_costs
            WHERE script = 'process_ai' AND incident_id IS NOT NULL
        )
    """ if ai_only else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*) FROM incidents i
            JOIN raw_incidents r
              ON r.jotform_submission_id = i.jotform_submission_id
            WHERE i.retracted = FALSE AND i.deleted_at IS NULL
            {ai_filter}
        """)
        return cur.fetchone()[0]


def show_compare(conn, limit: int, offset: int,
                 ai_only: bool, fmt_out: str, out_path: Optional[str]):
    records = fetch_compare_records(conn, limit, offset, ai_only)
    total   = count_compare_records(conn, ai_only)

    if not records:
        print('  No records found.')
        return

    if fmt_out == 'json':
        clean = []
        for r in records:
            row = {}
            for k, v in r.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()
                elif isinstance(v, (list, dict)):
                    row[k] = v
                else:
                    row[k] = str(v) if v is not None else None
            clean.append(row)
        output = json.dumps(clean, indent=2, ensure_ascii=False)
        print(output)
        if out_path:
            with open(out_path, 'w') as f:
                f.write(output)
            print(f'\n  Saved to: {out_path}')
        return

    if fmt_out == 'csv':
        rows = []
        for r in records:
            rows.append({
                'id':               r['id'],
                'date':             str(r['incident_date'])[:10] if r['incident_date'] else '',
                'city':             r.get('city') or '',
                'state':            r.get('state') or '',
                'raw_nature':       r.get('raw_nature') or '',
                'enriched_type':    r.get('incident_type') or '',
                'raw_severity':     str(r.get('raw_hate_crime') or ''),
                'enriched_severity':r.get('incident_severity') or '',
                'raw_victim':       (r.get('raw_victim') or '')[:100],
                'victim_religion':  r.get('victim_religion') or '',
                'victim_caste':     r.get('victim_caste') or '',
                'raw_suspect':      (r.get('raw_suspect') or '')[:100],
                'suspect_org':      r.get('suspect_org_affiliation') or '',
                'suspect_religion': r.get('suspect_religion') or '',
                'bias_motivation':  str(r.get('bias_motivation') or ''),
                'perpetrator_type': r.get('perpetrator_type') or '',
                'raw_fir':          r.get('raw_fir') or '',
                'fir_status':       r.get('fir_status') or '',
                'jotform_approval': r.get('enriched_jotform_approval') or '',
            })
        out = out_path or f'/tmp/hatewatch_audit_{_ts()}.csv'
        os.makedirs(os.path.dirname(out) if os.path.dirname(out) else '.', exist_ok=True)
        with open(out, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        # Also print table
        _print_compare_table(records, total, offset)
        print(f'  Saved to: {out}')
        return

    _print_compare_table(records, total, offset)


def _print_compare_table(records: list[dict], total: int, offset: int):
    print()
    print(f'  {"ID":<7} {"Date":<12} {"City":<16} '
          f'{"Raw nature":<20} {"→ Enriched type":<25} '
          f'{"Victim rel":<12} {"Suspect org":<22} {"Severity":<18}')
    print(f'  {"─"*7} {"─"*12} {"─"*16} '
          f'{"─"*20} {"─"*25} '
          f'{"─"*12} {"─"*22} {"─"*18}')

    for r in records:
        raw_nature  = fmt(r.get('raw_nature'), 20)
        enr_type    = fmt(r.get('incident_type'), 25)
        victim_rel  = fmt(r.get('victim_religion'), 12)
        suspect_org = fmt(r.get('suspect_org_affiliation'), 22)
        severity    = fmt(r.get('incident_severity'), 18)
        date        = str(r['incident_date'])[:10] if r['incident_date'] else '—'

        # Highlight changes
        changed = (r.get('incident_type') or r.get('victim_religion') or
                   r.get('suspect_org_affiliation') or r.get('incident_severity'))
        marker = '★' if changed else ' '

        print(f'  {marker}{r["id"]:<6} {date:<12} {fmt(r.get("city"), 16):<16} '
              f'{raw_nature:<20} {enr_type:<25} '
              f'{victim_rel:<12} {suspect_org:<22} {severity:<18}')

    print()
    print(f'  ★ = has enriched data   Showing {offset+1}–{min(offset+len(records), total)} of {total:,}')
    print()


# ---------------------------------------------------------------------------
# --id: full audit trail for one incident
# ---------------------------------------------------------------------------

def show_record_audit(conn, incident_id: int):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                i.*,
                r.raw_id,
                r.source_type,
                r.jotform_created_at,
                r.nature_of_incident        AS raw_nature,
                r.hate_crime_included       AS raw_hate_crime,
                r.victim_details_raw        AS raw_victim,
                r.suspect_details_raw       AS raw_suspect,
                r.fir_filed_raw,
                r.police_role_raw,
                r.address_raw,
                r.jotform_approval          AS raw_jotform_approval,
                r.motive_cause              AS raw_motive_cause
            FROM incidents i
            JOIN raw_incidents r
              ON r.jotform_submission_id = i.jotform_submission_id
            WHERE i.id = %s
        """, (incident_id,))
        rec = cur.fetchone()

    if not rec:
        print(f'  Incident #{incident_id} not found.')
        return

    rec = dict(rec)

    print()
    print('═' * 70)
    print(f'  AUDIT TRAIL — Incident #{incident_id}')
    print('═' * 70)
    print(f'  Date:      {rec.get("incident_date") or "—"}')
    print(f'  Location:  {rec.get("city") or "—"}, {rec.get("state") or "—"}')
    print(f'  Source:    {rec.get("source_type") or "—"} '
          f'(submitted: {(rec.get("jotform_created_at") or "?")[:10]})')
    print()

    # Field-by-field comparison
    print(f'  {"Field":<25} {"RAW (JotForm)":<35} {"ENRICHED":<30} {"By"}')
    print(f'  {"─"*25} {"─"*35} {"─"*30} {"─"*15}')

    comparisons = [
        ('incident_type',   rec.get('raw_nature'),          rec.get('incident_type'),           'rule+ai'),
        ('incident_severity',str(rec.get('raw_hate_crime') or '')[:30],
                                                             rec.get('incident_severity'),       'rule+ai'),
        ('bias_motivation', '—',                            str(rec.get('bias_motivation') or '—'), 'ai'),
        ('victim_religion', _extract_preview(rec.get('raw_victim'), 'Religion/Caste'),
                                                             rec.get('victim_religion'),         'rule+ai'),
        ('victim_caste',    _extract_preview(rec.get('raw_victim'), 'Religion/Caste', caste=True),
                                                             rec.get('victim_caste'),            'rule'),
        ('suspect_org',     _extract_preview(rec.get('raw_suspect'), 'Organizational Affiliation'),
                                                             rec.get('suspect_org_affiliation'), 'rule+ai'),
        ('suspect_religion',_extract_preview(rec.get('raw_suspect'), 'Religion/Caste'),
                                                             rec.get('suspect_religion'),        'rule'),
        ('perpetrator_type','—',                            rec.get('perpetrator_type'),         'ai'),
        ('motive_cause',    rec.get('raw_motive_cause'),    rec.get('motive_cause'),            'jotform'),
        ('fir_status',      rec.get('fir_filed_raw'),       rec.get('fir_status'),              'rule'),
        ('police_role',     rec.get('police_role_raw'),     rec.get('police_role'),             'rule'),
        ('state',           rec.get('address_raw','')[:30], rec.get('state'),                   'rule+geo'),
        ('jotform_approval',rec.get('raw_jotform_approval'),rec.get('jotform_approval'),        'csv'),
    ]

    for field, raw_val, enr_val, by in comparisons:
        raw_s = fmt(raw_val, 35) if raw_val else '—'
        enr_s = fmt(enr_val, 30) if enr_val else '—'
        changed = raw_val != enr_val and enr_val is not None
        marker = '→' if changed else ' '
        print(f'  {field:<25} {raw_s:<35} {marker} {enr_s:<29} {by}')

    # Verification log
    vlog = rec.get('verification_log')
    if vlog:
        entries = vlog if isinstance(vlog, list) else json.loads(vlog)
        print()
        print(f'  VERIFICATION LOG ({len(entries)} entries):')
        for entry in entries:
            ts     = str(entry.get('ts', ''))[:19]
            action = entry.get('action', '?')
            by     = entry.get('by', '?')
            conf   = entry.get('ai_confidence')
            model  = entry.get('ai_model', '')
            conf_s = f' (conf: {conf:.0%})' if conf else ''
            model_s= f' [{model}]' if model else ''
            print(f'    {ts}  {action:<20} by {by}{model_s}{conf_s}')

    # Description
    desc = rec.get('description') or ''
    if desc:
        print()
        print('  DESCRIPTION:')
        for line in textwrap.wrap(desc[:400], width=65):
            print(f'    {line}')

    print()
    print('═' * 70)
    print()


def _extract_preview(raw_json: str, field: str, caste: bool = False) -> Optional[str]:
    """Extract a field value from victim/suspect JSON for display."""
    if not raw_json:
        return None
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        if not isinstance(data, list):
            data = [data]
        vals = []
        for p in data:
            v = p.get(field, '')
            if v and v.lower() not in ('please select', 'unknown', 'unkown', ''):
                if caste and ' - ' in v:
                    vals.append(v.split(' - ', 1)[1].strip())
                elif not caste and ' - ' in v:
                    vals.append(v.split(' - ', 1)[0].strip())
                else:
                    vals.append(v)
        return ', '.join(dict.fromkeys(vals)) if vals else None
    except Exception:
        return str(raw_json)[:40] if raw_json else None


# ---------------------------------------------------------------------------
# --conflicts: where rule-based and AI disagree
# ---------------------------------------------------------------------------

def show_conflicts(conn, limit: int):
    """Show records where incident_type from rules differs from AI result."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                i.id,
                i.incident_date,
                i.city,
                i.state,
                r.nature_of_incident    AS raw_nature,
                i.incident_type         AS final_type,
                ac.result_summary       AS ai_summary
            FROM incidents i
            JOIN raw_incidents r
              ON r.jotform_submission_id = i.jotform_submission_id
            JOIN ai_costs ac ON ac.incident_id = i.id
            WHERE ac.script = 'process_ai'
            AND i.incident_type IS NOT NULL
            AND r.nature_of_incident IS NOT NULL
            AND i.incident_type::text NOT ILIKE
                REPLACE(LOWER(r.nature_of_incident), ' ', '_') || '%'
            ORDER BY i.id DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    print()
    print('=' * 65)
    print(f'  Potential conflicts — rule-based vs AI classification')
    print('=' * 65)
    if not rows:
        print('  No conflicts found — rule-based and AI agree on all records.')
    else:
        print(f'  {"ID":<7} {"Date":<12} {"City":<18} {"Raw nature":<22} {"Final type"}')
        print(f'  {"─"*7} {"─"*12} {"─"*18} {"─"*22} {"─"*25}')
        for r in rows:
            print(f'  {r["id"]:<7} {str(r["incident_date"])[:10]:<12} '
                  f'{fmt(r["city"],18):<18} {fmt(r["raw_nature"],22):<22} '
                  f'{r["final_type"]}')
    print('=' * 65)
    print()


# ---------------------------------------------------------------------------
# --stats: aggregate improvement metrics
# ---------------------------------------------------------------------------

def show_stats(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Total AI enrichment runs
        cur.execute("""
            SELECT
                COUNT(DISTINCT incident_id) as ai_records,
                SUM(cost_usd)               as total_cost,
                MIN(created_at)             as first_run,
                MAX(created_at)             as last_run
            FROM ai_costs
            WHERE script = 'process_ai'
        """)
        ai_stats = dict(cur.fetchone())

        # Fields populated BY AI (not in raw data)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(i.incident_type)          as type_filled,
                COUNT(i.incident_severity)      as severity_filled,
                COUNT(i.bias_motivation)        as motivation_filled,
                COUNT(i.perpetrator_type)       as perp_filled,
                COUNT(i.victim_religion)        as victim_rel_filled,
                COUNT(i.suspect_org_affiliation) as suspect_org_filled
            FROM incidents i
            WHERE i.retracted = FALSE
        """)
        coverage = dict(cur.fetchone())

        # Confidence distribution
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE result_summary LIKE 'confidence=0.%'
                    AND SUBSTRING(result_summary FROM 'confidence=([0-9.]+)')::float >= 0.8
                ) as high_conf,
                COUNT(*) FILTER (WHERE result_summary LIKE 'confidence=0.%'
                    AND SUBSTRING(result_summary FROM 'confidence=([0-9.]+)')::float >= 0.5
                    AND SUBSTRING(result_summary FROM 'confidence=([0-9.]+)')::float < 0.8
                ) as med_conf,
                COUNT(*) FILTER (WHERE result_summary LIKE 'confidence=0.%'
                    AND SUBSTRING(result_summary FROM 'confidence=([0-9.]+)')::float < 0.5
                ) as low_conf
            FROM ai_costs
            WHERE script = 'process_ai'
        """)
        conf_dist = dict(cur.fetchone())

    total = coverage['total'] or 1
    print()
    print('=' * 60)
    print('  HateWatch — AI Enrichment Statistics')
    print('=' * 60)
    print()
    print(f'  Records AI-enriched:     {ai_stats["ai_records"] or 0:,}')
    print(f'  Total AI cost:           ${float(ai_stats["total_cost"] or 0):.4f}')
    print(f'  First run:               {str(ai_stats["first_run"] or "—")[:19]}')
    print(f'  Last run:                {str(ai_stats["last_run"] or "—")[:19]}')
    print()
    print('  Field coverage after enrichment:')
    print(f'  {"Field":<25} {"Count":>8} {"Coverage":>10}')
    print(f'  {"─"*25} {"─"*8} {"─"*10}')
    fields = [
        ('incident_type',   'type_filled'),
        ('incident_severity','severity_filled'),
        ('bias_motivation', 'motivation_filled'),
        ('perpetrator_type','perp_filled'),
        ('victim_religion', 'victim_rel_filled'),
        ('suspect_org',     'suspect_org_filled'),
    ]
    for name, key in fields:
        count = coverage.get(key, 0)
        pct   = count / total * 100
        bar   = '█' * int(pct / 5)
        print(f'  {name:<25} {count:>8,} {pct:>9.1f}%  {bar}')

    print()
    print('  AI confidence distribution:')
    print(f'    High (80%+):   {conf_dist["high_conf"] or 0:,}')
    print(f'    Medium (50-80%):{conf_dist["med_conf"] or 0:,}')
    print(f'    Low (<50%):    {conf_dist["low_conf"] or 0:,}')
    print('=' * 60)
    print()


def _ts() -> str:
    return datetime.now().strftime('%Y%m%d_%H%M%S')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='HateWatch audit — compare raw vs enriched data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  audit.py --coverage
  audit.py --compare --limit 20
  audit.py --compare --ai-only
  audit.py --id 41614
  audit.py --conflicts
  audit.py --stats
  audit.py --compare -f csv -o /tmp/audit.csv
        """
    )

    parser.add_argument('--coverage',  action='store_true',
                        help='Show field completeness stats')
    parser.add_argument('--compare',   action='store_true',
                        help='Side-by-side raw vs enriched comparison')
    parser.add_argument('--ai-only',   action='store_true', dest='ai_only',
                        help='With --compare: only show AI-enriched records')
    parser.add_argument('--id',        type=int,
                        help='Full audit trail for one incident ID')
    parser.add_argument('--conflicts', action='store_true',
                        help='Show records where rule-based and AI disagree')
    parser.add_argument('--stats',     action='store_true',
                        help='Aggregate AI enrichment statistics')
    parser.add_argument('--limit',     type=int, default=50,
                        help='Max records to show (default: 50)')
    parser.add_argument('--offset',    type=int, default=0,
                        help='Pagination offset')
    parser.add_argument('--format', '-f', choices=['table', 'csv', 'json'],
                        default='table', help='Output format')
    parser.add_argument('--out', '-o', help='Output file path')

    args = parser.parse_args()
    conn = get_conn()

    if args.coverage:
        show_coverage(conn)
    elif args.compare:
        show_compare(conn, args.limit, args.offset,
                     args.ai_only, args.format, args.out)
    elif args.id:
        show_record_audit(conn, args.id)
    elif args.conflicts:
        show_conflicts(conn, args.limit)
    elif args.stats:
        show_stats(conn)
    else:
        # Default: show coverage
        show_coverage(conn)

    conn.close()
