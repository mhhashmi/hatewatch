#!/usr/bin/env python3
"""
ai_verify.py — HateWatch AI verification pipeline (Stage 2)
============================================================
Uses Claude with web search to verify incident reports against
publicly available news sources. Tracks costs in ai_costs table.

Usage:
    uv run python pipeline/ai_verify.py                        # dry-run, 5 records
    uv run python pipeline/ai_verify.py --fix                  # verify + write to DB
    uv run python pipeline/ai_verify.py --fix --limit 10       # verify 10 records
    uv run python pipeline/ai_verify.py --fix --limit 50 --budget 2.00  # stop at $2
    uv run python pipeline/ai_verify.py --fix --all            # verify all (caution!)
    uv run python pipeline/ai_verify.py --costs                # show cost report only
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

import anthropic
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
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/ai_verify.log'),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL    = os.environ.get('DATABASE_URL')
ANTHROPIC_KEY   = os.environ.get('ANTHROPIC_API_KEY')

if not DATABASE_URL:
    raise SystemExit('ERROR: DATABASE_URL missing from .env')
if not ANTHROPIC_KEY:
    raise SystemExit('ERROR: ANTHROPIC_API_KEY missing from .env')

# Claude Sonnet pricing (per million tokens) — update if pricing changes
# Current as of mid-2025
PRICE_INPUT_PER_M  = 3.00   # $3.00 per 1M input tokens
PRICE_OUTPUT_PER_M = 15.00  # $15.00 per 1M output tokens

MODEL = 'claude-sonnet-4-6'

# ---------------------------------------------------------------------------
# Cost tracking helpers
# ---------------------------------------------------------------------------

def calculate_cost(input_tokens: int, output_tokens: int) -> float:
    return (
        (input_tokens  / 1_000_000) * PRICE_INPUT_PER_M +
        (output_tokens / 1_000_000) * PRICE_OUTPUT_PER_M
    )


def get_total_spent(conn) -> float:
    """Return total USD spent on AI verification to date."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(cost_usd), 0)
                FROM ai_costs
                WHERE script = 'ai_verify'
            """)
            return float(cur.fetchone()[0])
    except Exception:
        return 0.0


def log_cost(conn, incident_id: int, input_tokens: int,
             output_tokens: int, cost: float, model: str,
             result_summary: str, dry_run: bool):
    if dry_run:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_costs
                    (incident_id, script, model, input_tokens,
                     output_tokens, cost_usd, result_summary, created_at)
                VALUES (%s, 'ai_verify', %s, %s, %s, %s, %s, NOW())
            """, (incident_id, model, input_tokens,
                  output_tokens, cost, result_summary[:200]))
        conn.commit()
    except Exception as e:
        log.warning('Could not log cost: %s', e)


def show_cost_report(conn):
    """Print a full cost report from the ai_costs table."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    DATE(created_at) as date,
                    COUNT(*) as records,
                    SUM(input_tokens) as input_tokens,
                    SUM(output_tokens) as output_tokens,
                    SUM(cost_usd) as cost_usd
                FROM ai_costs
                WHERE script = 'ai_verify'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 30
            """)
            rows = cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*) as total_records,
                    SUM(cost_usd) as total_cost
                FROM ai_costs
                WHERE script = 'ai_verify'
            """)
            totals = cur.fetchone()

        print()
        print('=' * 55)
        print('  AI Verification Cost Report')
        print('=' * 55)
        print(f'  {"Date":<12} {"Records":>8} {"Input tok":>10} {"Output tok":>11} {"Cost":>8}')
        print(f'  {"-"*12} {"-"*8} {"-"*10} {"-"*11} {"-"*8}')
        for r in rows:
            print(f'  {str(r["date"]):<12} {r["records"]:>8,} '
                  f'{r["input_tokens"]:>10,} {r["output_tokens"]:>11,} '
                  f'${r["cost_usd"]:>7.4f}')
        print(f'  {"-"*12} {"-"*8} {"-"*10} {"-"*11} {"-"*8}')
        print(f'  {"TOTAL":<12} {totals["total_records"]:>8,} '
              f'{"":>10} {"":>11} ${totals["total_cost"]:>7.4f}')
        print('=' * 55)
        print()
    except Exception as e:
        log.error('Cost report error: %s', e)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def ensure_ai_costs_table(conn):
    """Create ai_costs table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_costs (
                id              BIGSERIAL PRIMARY KEY,
                incident_id     BIGINT REFERENCES incidents(id) ON DELETE SET NULL,
                script          TEXT NOT NULL,
                model           TEXT NOT NULL,
                input_tokens    INTEGER,
                output_tokens   INTEGER,
                cost_usd        NUMERIC(10, 6),
                result_summary  TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ai_costs_script
            ON ai_costs(script, created_at)
        """)
    conn.commit()


def fetch_records_to_verify(conn, limit: int, source: str = 'both') -> list[dict]:
    """
    Fetch incidents ready for AI verification.
    source: 'approved' | 'not_approved' | 'both'
    Priority: JotForm-approved first, then others.
    Excludes already verified and retracted records.
    Excludes records already processed by ai_verify.
    """
    # Build approval filter
    if source == 'approved':
        approval_filter = "AND i.approval_status = 'approved'"
    elif source == 'not_approved':
        approval_filter = "AND i.approval_status != 'approved'"
    else:
        approval_filter = ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                i.id, i.jotform_submission_id,
                i.incident_date, i.incident_type,
                i.description, i.nature_of_incident,
                i.state, i.city, i.district,
                i.casualties, i.injured,
                i.fir_status, i.fir_charges,
                i.police_role, i.approval_status,
                i.verification_status
            FROM incidents i
            WHERE i.verification_status = 'in_review'
            AND i.retracted = FALSE
            AND i.deleted_at IS NULL
            AND i.description IS NOT NULL
            AND LENGTH(i.description) > 50
            AND i.id NOT IN (
                SELECT DISTINCT incident_id FROM ai_costs
                WHERE script = 'ai_verify'
                AND incident_id IS NOT NULL
            )
            {approval_filter}
            ORDER BY
                CASE WHEN i.approval_status = 'approved' THEN 0 ELSE 1 END,
                i.incident_date DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]


def save_verification_result(conn, incident_id: int, result: dict, dry_run: bool):
    """Save AI verification result to ai_staging and update incident."""
    if dry_run:
        return

    # Write to ai_staging
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ai_staging
                (source_url, source_type, raw_json, ai_provider, ai_model,
                 ai_prompt_version, confidence_score, extraction_notes,
                 target_table, review_status, created_at)
            VALUES (%s, 'news_article', %s, 'anthropic', %s,
                    'v1.0', %s, %s, 'incidents', 'pending', NOW())
            RETURNING id
        """, (
            result.get('primary_source_url'),
            json.dumps(result),
            MODEL,
            result.get('confidence_score'),
            result.get('discrepancies_summary'),
        ))
        staging_id = cur.fetchone()[0]

    # Update incident reliability_level based on confidence
    confidence = result.get('confidence_score', 0)
    if confidence >= 0.8:
        new_reliability = '2'  # corroborated
        new_status = 'verified'
    elif confidence >= 0.5:
        new_reliability = '2'
        new_status = 'in_review'  # keep for human review
    else:
        new_reliability = '1'
        new_status = 'needs_sources'

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE incidents SET
                reliability_level   = %s,
                verification_status = %s,
                review_notes        = %s,
                updated_at          = NOW()
            WHERE id = %s
        """, (
            new_reliability,
            new_status,
            f'AI verified (confidence: {confidence:.0%}). '
            f'Sources found: {result.get("sources_found", 0)}. '
            f'{result.get("discrepancies_summary", "")}',
            incident_id,
        ))

    conn.commit()


# ---------------------------------------------------------------------------
# AI verification prompt
# ---------------------------------------------------------------------------

VERIFICATION_PROMPT = """You are verifying an incident report for a human rights database.

INCIDENT:
{incident_json}

Do ONE web search to find news coverage of this incident. Use date + location + key detail as search terms.

YOU MUST RESPOND WITH ONLY THIS JSON — NO OTHER TEXT BEFORE OR AFTER:

{{"confidence_score": 0.0, "verified": false, "sources_found": 0, "primary_source_url": "", "corroborating_urls": [], "date_accurate": null, "location_accurate": null, "incident_type_accurate": null, "discrepancies": [], "discrepancies_summary": "", "additional_context": "", "search_queries_used": []}}

Rules:
- confidence_score: 0.0 (no sources found) to 1.0 (multiple sources confirm all facts)
- verified: true only if 2+ independent sources confirm the core facts
- Keep discrepancies_summary under 200 characters
- Keep additional_context under 200 characters
- Do not invent URLs — use empty string if not found
- If no sources found, return confidence_score 0.0 and empty URLs"""


def verify_incident(client: anthropic.Anthropic, record: dict) -> tuple[dict, int, int]:
    """
    Verify a single incident using Claude with web search.
    Returns (result_dict, input_tokens, output_tokens).
    """
    # Build incident summary for the prompt — keep short to control token cost
    incident_summary = {
        'date':         str(record.get('incident_date', 'unknown')),
        'location':     f"{record.get('city', '')}, {record.get('state', '')}".strip(', '),
        'type':         record.get('incident_type') or record.get('nature_of_incident', ''),
        'description':  (record.get('description', '') or '')[:300],  # strict cap
        'fir_status':   record.get('fir_status', ''),
    }

    prompt = VERIFICATION_PROMPT.format(
        incident_json=json.dumps(incident_summary, indent=2)
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=600,
        tools=[{
            'type': 'web_search_20250305',
            'name': 'web_search',
            'max_uses': 1,                    # only 1 search per record
        }],
        messages=[{'role': 'user', 'content': prompt}],
    )

    # Extract text response
    full_text = ' '.join(
        block.text for block in response.content
        if hasattr(block, 'text')
    )

    # Parse JSON result
    try:
        # Find JSON in response
        start = full_text.find('{')
        end   = full_text.rfind('}') + 1
        if start >= 0 and end > start:
            result = json.loads(full_text[start:end])
        else:
            raise ValueError('No JSON found in response')
    except Exception as e:
        log.warning('JSON parse error for incident %s: %s', record['id'], e)
        result = {
            'confidence_score':     0.0,
            'verified':             False,
            'sources_found':        0,
            'primary_source_url':   '',
            'corroborating_urls':   [],
            'discrepancies':        [],
            'discrepancies_summary': f'Parse error: {e}',
            'additional_context':   full_text[:500],
            'search_queries_used':  [],
        }

    input_tokens  = response.usage.input_tokens
    output_tokens = response.usage.output_tokens

    return result, input_tokens, output_tokens


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def show_verification_summary(conn):
    """Show a full breakdown of verification status across all records."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Overall verification status
        cur.execute("""
            SELECT
                verification_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
            FROM incidents
            WHERE retracted = FALSE AND deleted_at IS NULL
            GROUP BY verification_status
            ORDER BY count DESC
        """)
        statuses = cur.fetchall()

        # JotForm approved vs not — breakdown
        cur.execute("""
            SELECT
                approval_status,
                verification_status,
                COUNT(*) as count
            FROM incidents
            WHERE retracted = FALSE AND deleted_at IS NULL
            GROUP BY approval_status, verification_status
            ORDER BY approval_status, count DESC
        """)
        breakdown = cur.fetchall()

        # Already AI verified (from ai_costs table)
        cur.execute("""
            SELECT
                COUNT(DISTINCT incident_id) as ai_verified,
                ROUND(AVG(
                    CASE WHEN result_summary LIKE 'confidence=%'
                    THEN SUBSTRING(result_summary FROM 'confidence=([0-9.]+)')::numeric
                    ELSE NULL END
                )::numeric, 3) as avg_confidence
            FROM ai_costs
            WHERE script = 'ai_verify'
            AND incident_id IS NOT NULL
        """)
        ai_stats = cur.fetchone()

        # Eligible for AI verification
        cur.execute("""
            SELECT
                approval_status,
                COUNT(*) as eligible
            FROM incidents
            WHERE verification_status = 'in_review'
            AND retracted = FALSE
            AND deleted_at IS NULL
            AND description IS NOT NULL
            AND LENGTH(description) > 50
            AND id NOT IN (
                SELECT DISTINCT incident_id FROM ai_costs
                WHERE script = 'ai_verify' AND incident_id IS NOT NULL
            )
            GROUP BY approval_status
            ORDER BY eligible DESC
        """)
        eligible = cur.fetchall()

    print()
    print('=' * 60)
    print('  HateWatch — Verification Summary')
    print('=' * 60)

    print()
    print('  Overall verification status:')
    for r in statuses:
        bar = '█' * int(float(r['pct']) / 5)
        print(f'    {r["verification_status"]:<20} {r["count"]:>6,}  ({r["pct"]}%)  {bar}')

    print()
    print('  JotForm approval vs verification breakdown:')
    print(f'  {"Approval":<15} {"Verification":<20} {"Count":>6}')
    print(f'  {"-"*15} {"-"*20} {"-"*6}')
    for r in breakdown:
        print(f'  {str(r["approval_status"]):<15} {str(r["verification_status"]):<20} {r["count"]:>6,}')

    print()
    print('  AI verification progress:')
    print(f'    Already AI verified:  {ai_stats["ai_verified"] or 0:,}')
    if ai_stats["avg_confidence"]:
        print(f'    Avg confidence score: {float(ai_stats["avg_confidence"]):.0%}')

    print()
    print('  Eligible for AI verification (not yet processed):')
    total_eligible = sum(r['eligible'] for r in eligible)
    for r in eligible:
        label = f'JotForm {r["approval_status"]}'
        print(f'    {label:<30} {r["eligible"]:>6,}')
    print(f'    {"TOTAL":<30} {total_eligible:>6,}')

    # Cost estimate
    est_low  = total_eligible * 0.015
    est_high = total_eligible * 0.035
    print()
    print(f'  Estimated cost to verify all eligible: ${est_low:.0f}–${est_high:.0f}')
    print('=' * 60)
    print()
    print('  Flags to filter by source:')
    print('    --source approved      # JotForm-approved records only')
    print('    --source not_approved  # pending/in_progress records only')
    print('    --source both          # all eligible (default)')
    print()


def generate_report(results: list[dict], run_cost: float,
                    spent_total: float, dry_run: bool) -> str:
    """
    Generate a human-readable verification report.
    Returns the report as a string (written to screen + log file).
    """
    now  = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    mode = 'DRY RUN' if dry_run else 'LIVE'

    lines = []
    lines.append('=' * 60)
    lines.append('  HateWatch — AI Verification Report')
    lines.append(f'  Run date:   {now}')
    lines.append(f'  Mode:       {mode}')
    lines.append(f'  Records:    {len(results)} verified')
    lines.append(f'  Cost:       ${run_cost:.4f}  (total to date: ${spent_total:.4f})')
    lines.append('=' * 60)
    lines.append('')

    status_counts = {}
    for i, r in enumerate(results, 1):
        lines.append(f'RECORD {i} — ID {r["id"]} | '
                     f'{r.get("city","?")}, {r.get("state","?")} | '
                     f'{r.get("incident_date","?")}')

        old_s = r.get('old_status', 'in_review')
        new_s = r.get('new_status', 'in_review')
        conf  = r.get('confidence', 0)
        nsrc  = r.get('sources_found', 0)

        lines.append(f'  Status change:  {old_s} → {new_s}')
        lines.append(f'  Confidence:     {conf:.0%}')
        lines.append(f'  Sources found:  {nsrc}')

        if r.get('primary_source_url'):
            lines.append(f'  Primary source: {r["primary_source_url"]}')
        for url in (r.get('corroborating_urls') or [])[:2]:
            lines.append(f'  Also found:     {url}')

        finding = r.get('finding', '')
        if finding:
            # Word-wrap finding at 55 chars
            words   = finding.split()
            line_buf = '  Finding:        '
            for word in words:
                if len(line_buf) + len(word) + 1 > 58:
                    lines.append(line_buf)
                    line_buf = '                  ' + word + ' '
                else:
                    line_buf += word + ' '
            if line_buf.strip():
                lines.append(line_buf.rstrip())

        if r.get('error'):
            lines.append(f'  ⚠ Error:        {r["error"]}')

        lines.append('')
        status_counts[new_s] = status_counts.get(new_s, 0) + 1

    lines.append('=' * 60)
    lines.append('  SUMMARY')
    lines.append('=' * 60)
    for status, count in sorted(status_counts.items()):
        arrow = '✓' if status == 'verified' else '⚠' if status == 'needs_sources' else '→'
        lines.append(f'  {arrow} {status:<22} {count:>4} records')
    lines.append('')
    lines.append(f'  This run cost:      ${run_cost:.4f}')
    lines.append(f'  Total spent:        ${spent_total:.4f}')
    lines.append('=' * 60)

    return '\n'.join(lines)


def save_report(report_text: str) -> str:
    """Save report to timestamped log file. Returns filename."""
    os.makedirs('logs', exist_ok=True)
    ts       = datetime.now().strftime('%Y-%m-%d_%H%M')
    filename = f'logs/ai_verify_{ts}.log'
    with open(filename, 'w') as f:
        f.write(report_text)
        f.write('\n')
    return filename


def run(dry_run: bool, limit: int, budget: Optional[float], show_costs: bool,
        summary: bool = False, source: str = 'both'):
    mode = 'DRY RUN' if dry_run else 'LIVE'
    log.info('=== ai_verify.py started (%s) ===', mode)

    conn   = psycopg2.connect(DATABASE_URL)
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    ensure_ai_costs_table(conn)

    if show_costs:
        show_cost_report(conn)
        conn.close()
        return

    if summary:
        show_verification_summary(conn)
        conn.close()
        return

    # Get baseline costs
    spent_before = get_total_spent(conn)

    # Fetch records
    records = fetch_records_to_verify(conn, limit, source=source)
    log.info('Records to verify: %d (limit: %d)', len(records), limit)

    if not records:
        log.info('No records to verify — all up to date!')
        conn.close()
        return

    # Estimate cost before starting
    est_cost_per_record = 0.025  # conservative estimate
    est_total = len(records) * est_cost_per_record
    projected_full = 7445 * est_cost_per_record  # rough full backlog estimate

    print()
    print('=' * 55)
    print(f'  AI Verification — Pre-run estimate')
    print('=' * 55)
    print(f'  Records to verify:     {len(records)}')
    print(f'  Est. cost this run:    ${est_total:.2f}–${est_total*2:.2f}')
    print(f'  Budget limit:          {"$" + str(budget) if budget else "none"}')
    print(f'  Total spent to date:   ${spent_before:.4f}')
    print(f'  Projected full backlog: ${projected_full:.0f}–${projected_full*2:.0f}')
    print('=' * 55)

    if dry_run:
        print()
        print('  DRY RUN — showing first 2 records that would be verified:')
        for r in records[:2]:
            print(f'    ID {r["id"]}: {r.get("incident_date")} | '
                  f'{r.get("city")}, {r.get("state")} | '
                  f'{(r.get("description") or "")[:60]}...')
        print()
        print('  Run with --fix to start verification')
        print()
        conn.close()
        return

    # Process records
    run_input_tokens  = 0
    run_output_tokens = 0
    run_cost          = 0.0
    verified_count    = 0
    failed_count      = 0
    budget_hit        = False

    run_results = []

    for i, record in enumerate(records, 1):
        # Check budget
        if budget and (spent_before + run_cost) >= budget:
            log.warning('Budget limit $%.2f reached — stopping', budget)
            budget_hit = True
            break

        log.info('Verifying %d/%d: ID %d | %s | %s, %s',
                 i, len(records), record['id'],
                 record.get('incident_date', '?'),
                 record.get('city', '?'),
                 record.get('state', '?'))

        try:
            result, inp_tok, out_tok = verify_incident(client, record)
            cost = calculate_cost(inp_tok, out_tok)

            run_input_tokens  += inp_tok
            run_output_tokens += out_tok
            run_cost          += cost

            conf = result.get('confidence_score', 0)
            nsrc = result.get('sources_found', 0)
            log.info('  Confidence: %.0f%% | Sources: %d | Cost: $%.4f',
                     conf * 100, nsrc, cost)

            # Determine new status for report
            if conf >= 0.8:
                new_status = 'verified'
            elif conf >= 0.5:
                new_status = 'in_review'
            else:
                new_status = 'needs_sources'

            # Collect for report
            run_results.append({
                'id':                  record['id'],
                'city':                record.get('city'),
                'state':               record.get('state'),
                'incident_date':       str(record.get('incident_date', '')),
                'old_status':          record.get('verification_status', 'in_review'),
                'new_status':          new_status,
                'confidence':          conf,
                'sources_found':       nsrc,
                'primary_source_url':  result.get('primary_source_url', ''),
                'corroborating_urls':  result.get('corroborating_urls', []),
                'finding':             (result.get('discrepancies_summary') or
                                        result.get('additional_context', ''))[:300],
            })

            save_verification_result(conn, record['id'], result, dry_run)
            log_cost(conn, record['id'], inp_tok, out_tok, cost, MODEL,
                     f"confidence={conf:.2f} sources={nsrc}",
                     dry_run)

            verified_count += 1

        except Exception as e:
            failed_count += 1
            run_results.append({
                'id':         record['id'],
                'city':       record.get('city'),
                'state':      record.get('state'),
                'incident_date': str(record.get('incident_date', '')),
                'old_status': record.get('verification_status', 'in_review'),
                'new_status': 'in_review',
                'confidence': 0,
                'sources_found': 0,
                'error':      str(e)[:150],
            })
            log.error('  FAILED: %s', e)
            continue

    # Generate and save report
    spent_after = get_total_spent(conn)

    if run_results:
        report_text = generate_report(run_results, run_cost, spent_after, dry_run)
        print()
        print(report_text)

        if budget_hit:
            print(f'  ⚠ Budget limit ${budget:.2f} reached — {verified_count} records processed')
            print()

        print(f'  Input tokens:   {run_input_tokens:,}')
        print(f'  Output tokens:  {run_output_tokens:,}')
        print()

        log_file = save_report(report_text)
        print(f'📄 Report saved to: {log_file}')
        print(f'   Run --costs to see full cost history')
        print()

    conn.close()
    log.info('=== ai_verify.py complete ===')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='HateWatch AI verification pipeline',
        epilog='Default: dry-run, 5 records. Use --fix to write to DB.',
    )
    parser.add_argument('--fix',    action='store_true',
                        help='Write results to DB (default: dry-run)')
    parser.add_argument('--limit',  type=int, default=5,
                        help='Max records to verify (default: 5)')
    parser.add_argument('--budget', type=float, default=None,
                        help='Stop if cumulative cost exceeds this USD amount')
    parser.add_argument('--all',    action='store_true',
                        help='Verify all eligible records (overrides --limit)')
    parser.add_argument('--costs',  action='store_true',
                        help='Show cost report and exit')
    parser.add_argument('--summary', action='store_true',
                        help='Show verification summary stats and exit (no API calls)')
    parser.add_argument('--source', choices=['approved', 'not_approved', 'both'],
                        default='both',
                        help='Which records to verify: approved (JotForm approved only), '
                             'not_approved (pending/in_progress only), or both (default)')
    args = parser.parse_args()

    if args.all:
        args.limit = 99999

    run(
        dry_run    = not args.fix,
        limit      = args.limit,
        budget     = args.budget,
        show_costs = args.costs,
        summary    = args.summary,
        source     = args.source,
    )