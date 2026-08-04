#!/usr/bin/env python3
"""
validate.py — HateWatch data quality validator
===============================================
Scans incidents table for data quality issues.
By default runs in DRY-RUN mode — reports problems without touching the DB.

Usage:
    uv run python validate.py                        # dry-run report
    uv run python validate.py --fix                  # update verification_status in DB
    uv run python validate.py --fix --batch 500      # process in batches of 500
    uv run python validate.py --fix --limit 100      # fix first 100 only (for testing)
    uv run python validate.py --report report.json   # save full report to JSON file
    uv run python validate.py --fix --report out.json  # fix + save report
"""

import os
import sys
import json
import logging
import argparse
from datetime import date, datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
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
# Validation rules
# Each rule returns (passed: bool, issue: str | None)
# ---------------------------------------------------------------------------

TODAY = date.today()
MIN_DESCRIPTION_LEN = 30
MAX_CASUALTIES = 500
MAX_INJURED = 2000


def check_has_date(row: dict) -> tuple[bool, Optional[str]]:
    if not row.get('incident_date'):
        return False, 'missing incident_date'
    return True, None


def check_date_not_future(row: dict) -> tuple[bool, Optional[str]]:
    d = row.get('incident_date')
    if not d:
        return True, None  # already caught by check_has_date
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d)
        except ValueError:
            return False, f'unparseable date: {d}'
    if d > TODAY:
        return False, f'future date: {d}'
    return True, None


def check_has_description(row: dict) -> tuple[bool, Optional[str]]:
    desc = row.get('description') or ''
    if not desc.strip():
        return False, 'missing description'
    if len(desc.strip()) < MIN_DESCRIPTION_LEN:
        return False, f'description too short ({len(desc.strip())} chars, min {MIN_DESCRIPTION_LEN})'
    return True, None


def check_has_location(row: dict) -> tuple[bool, Optional[str]]:
    has_state = bool(row.get('state'))
    has_city  = bool(row.get('city'))
    has_geo   = bool(row.get('latitude'))
    has_addr  = bool(row.get('address') or row.get('geolocation_raw'))
    if not any([has_state, has_city, has_geo, has_addr]):
        return False, 'no location data (state/city/coordinates/address all missing)'
    if not has_state:
        return True, 'state missing — has city/geo but state not extracted yet'
    return True, None


def check_casualties_sane(row: dict) -> tuple[bool, Optional[str]]:
    c = row.get('casualties')
    if c is not None and c > MAX_CASUALTIES:
        return False, f'casualties={c} exceeds maximum ({MAX_CASUALTIES}) — likely data entry error'
    i = row.get('injured')
    if i is not None and i > MAX_INJURED:
        return False, f'injured={i} exceeds maximum ({MAX_INJURED}) — likely data entry error'
    return True, None


def check_has_incident_type(row: dict) -> tuple[bool, Optional[str]]:
    if not row.get('incident_type') and not row.get('nature_of_incident'):
        return False, 'no incident type or nature of incident'
    return True, None


def check_fir_consistency(row: dict) -> tuple[bool, Optional[str]]:
    """If FIR is filed, there should be a date or charges."""
    if row.get('fir_status') == 'filed':
        has_date    = bool(row.get('fir_filed_date'))
        has_charges = bool(row.get('fir_charges'))
        has_station = bool(row.get('police_station'))
        if not any([has_date, has_charges, has_station]):
            return True, 'FIR marked filed but no date/charges/station recorded'
    return True, None


def check_approval_status(row: dict) -> tuple[bool, Optional[str]]:
    """JotForm-approved records should be promoted."""
    if row.get('approval_status') == 'approved':
        if row.get('verification_status') == 'pending':
            return True, 'JotForm-approved but verification_status still pending — should be in_review'
    return True, None


# ---------------------------------------------------------------------------
# All rules in priority order
# (critical rules first — they affect verification_status)
# ---------------------------------------------------------------------------
CRITICAL_RULES = [
    check_has_date,
    check_date_not_future,
    check_has_description,
    check_casualties_sane,
]

WARNING_RULES = [
    check_has_location,
    check_has_incident_type,
    check_fir_consistency,
    check_approval_status,
]


def validate_record(row: dict) -> dict:
    """
    Run all rules against a record.
    Returns a result dict with:
        passed        — True if no critical failures
        critical      — list of critical issue strings
        warnings      — list of warning strings
        suggested_status — what verification_status should be set to
    """
    critical = []
    warnings = []

    for rule in CRITICAL_RULES:
        ok, issue = rule(row)
        if not ok and issue:
            critical.append(issue)

    for rule in WARNING_RULES:
        ok, issue = rule(row)
        if issue:  # warnings always noted even if ok=True
            warnings.append(issue)

    passed = len(critical) == 0

    # Determine suggested verification_status
    current_approval = row.get('approval_status', 'pending')
    current_verification = row.get('verification_status', 'pending')

    if current_verification in ('verified', 'published', 'retracted'):
        suggested = current_verification  # don't downgrade verified records
    elif not passed:
        suggested = 'needs_sources'
    elif current_approval == 'approved':
        suggested = 'in_review'  # JotForm approved → ready for AI check
    elif current_approval == 'rejected':
        suggested = 'retracted'
    else:
        suggested = 'in_review'  # passes quality checks → ready for review

    return {
        'id':                 row['id'],
        'jotform_id':         row.get('jotform_submission_id'),
        'passed':             passed,
        'critical':           critical,
        'warnings':           warnings,
        'current_status':     current_verification,
        'suggested_status':   suggested,
        'approval_status':    current_approval,
    }


# ---------------------------------------------------------------------------
# Batch DB operations
# ---------------------------------------------------------------------------

def fetch_records(conn, batch_size: int, offset: int) -> list:
    """Fetch a batch of incidents for validation."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                id, jotform_submission_id, incident_date, description,
                state, city, address, latitude, geolocation_raw,
                casualties, injured, incident_type, nature_of_incident,
                fir_status, fir_filed_date, fir_charges, police_station,
                approval_status, verification_status
            FROM incidents
            WHERE deleted_at IS NULL
            ORDER BY id
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        return [dict(r) for r in cur.fetchall()]


def count_records(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM incidents WHERE deleted_at IS NULL")
        return cur.fetchone()[0]


def apply_updates(conn, updates: list[dict], dry_run: bool) -> int:
    """
    Batch-update verification_status for validated records.
    updates: list of {id, suggested_status}
    Returns number of records updated.
    """
    if dry_run or not updates:
        return 0

    # Only update records where status actually changes
    to_update = [
        (u['suggested_status'], u['id'])
        for u in updates
        if u['suggested_status'] != u['current_status']
    ]

    if not to_update:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE incidents
            SET verification_status = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            to_update,
            page_size=500,
        )
    conn.commit()
    return len(to_update)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_report(all_results: list[dict], total: int, duration: float, dry_run: bool) -> dict:
    passed       = [r for r in all_results if r['passed']]
    failed       = [r for r in all_results if not r['passed']]
    approved     = [r for r in all_results if r['approval_status'] == 'approved']
    rejected     = [r for r in all_results if r['approval_status'] == 'rejected']
    in_progress  = [r for r in all_results if r['approval_status'] == 'in_review']
    needs_src    = [r for r in all_results if r['suggested_status'] == 'needs_sources']
    ready_for_ai = [r for r in all_results if r['suggested_status'] == 'in_review']

    # Collect all unique issues
    issue_counts = {}
    for r in all_results:
        for issue in r['critical'] + r['warnings']:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1

    # Sort issues by frequency
    top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)

    return {
        'generated_at':     datetime.now(timezone.utc).isoformat(),
        'dry_run':          dry_run,
        'duration_seconds': round(duration, 1),
        'summary': {
            'total':              total,
            'scanned':            len(all_results),
            'passed_quality':     len(passed),
            'failed_quality':     len(failed),
            'jotform_approved':   len(approved),
            'jotform_rejected':   len(rejected),
            'jotform_in_progress':len(in_progress),
            'needs_sources':      len(needs_src),
            'ready_for_ai_check': len(ready_for_ai),
        },
        'top_issues':   top_issues[:20],
        'failed_ids':   [r['id'] for r in failed][:100],  # first 100 failed IDs
    }


def print_report(report: dict):
    s = report['summary']
    total = s['total']

    def pct(n):
        return f"{n/total*100:.1f}%" if total else "0%"

    print()
    print("=" * 60)
    print("  HateWatch — Data Quality Report")
    print(f"  Generated: {report['generated_at'][:19]}")
    print(f"  Mode: {'DRY RUN — no changes made' if report['dry_run'] else 'LIVE — DB updated'}")
    print("=" * 60)
    print(f"  Total records:          {s['total']:>6,}")
    print(f"  Scanned:                {s['scanned']:>6,}")
    print()
    print(f"  ✓ Passed quality check: {s['passed_quality']:>6,}  ({pct(s['passed_quality'])})")
    print(f"  ✗ Failed quality check: {s['failed_quality']:>6,}  ({pct(s['failed_quality'])})")
    print()
    print(f"  JotForm approved:       {s['jotform_approved']:>6,}  ({pct(s['jotform_approved'])})")
    print(f"  JotForm rejected:       {s['jotform_rejected']:>6,}  ({pct(s['jotform_rejected'])})")
    print(f"  JotForm in progress:    {s['jotform_in_progress']:>6,}  ({pct(s['jotform_in_progress'])})")
    print()
    print(f"  Needs sources/fixes:    {s['needs_sources']:>6,}  ({pct(s['needs_sources'])})")
    print(f"  Ready for AI check:     {s['ready_for_ai_check']:>6,}  ({pct(s['ready_for_ai_check'])})")
    print()
    print("  Top issues found:")
    for issue, count in report['top_issues'][:10]:
        print(f"    {count:>5,}  {issue}")
    print()
    print(f"  Duration: {report['duration_seconds']}s")
    print("=" * 60)
    if report['dry_run']:
        print("  Run with --fix to apply suggested status updates to DB")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool, batch_size: int, limit: Optional[int], report_path: Optional[str]):
    start = datetime.now(timezone.utc)
    mode  = 'DRY RUN' if dry_run else 'LIVE'
    log.info('=== HateWatch validate.py started (%s) ===', mode)

    conn  = psycopg2.connect(DATABASE_URL)
    total = count_records(conn)
    log.info('Total records to scan: %d', total)

    cap          = limit or total
    all_results  = []
    total_updated= 0
    offset       = 0

    while offset < cap:
        size  = min(batch_size, cap - offset)
        batch = fetch_records(conn, size, offset)
        if not batch:
            break

        results = [validate_record(row) for row in batch]
        all_results.extend(results)

        updated = apply_updates(conn, results, dry_run)
        total_updated += updated

        log.info(
            'Batch %d–%d: %d passed, %d failed, %d updated',
            offset + 1, offset + len(batch),
            sum(1 for r in results if r['passed']),
            sum(1 for r in results if not r['passed']),
            updated,
        )

        offset += len(batch)

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    report   = build_report(all_results, total, duration, dry_run)

    print_report(report)

    if report_path:
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        log.info('Report saved to %s', report_path)

    if not dry_run:
        log.info('Records updated in DB: %d', total_updated)

    conn.close()
    log.info('=== validate.py complete ===')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='HateWatch data quality validator',
        epilog='Default mode is DRY RUN — use --fix to apply changes to DB',
    )
    parser.add_argument(
        '--fix',
        action='store_true',
        help='Apply suggested verification_status updates to DB (default: dry-run)',
    )
    parser.add_argument(
        '--batch',
        type=int,
        default=500,
        help='Records per batch (default: 500)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Only scan first N records (useful for testing)',
    )
    parser.add_argument(
        '--report',
        type=str,
        default=None,
        metavar='PATH',
        help='Save full report to JSON file (e.g. --report report.json)',
    )
    args = parser.parse_args()

    run(
        dry_run=not args.fix,
        batch_size=args.batch,
        limit=args.limit,
        report_path=args.report,
    )