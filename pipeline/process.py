#!/usr/bin/env python3
"""
process.py — HateWatch raw_incidents → incidents enrichment pipeline
=====================================================================
Reads raw_incidents, enriches the data, writes to incidents.

Stage 1 — Rule-based (free, instant):
  - Parse victim/suspect JSON → structured fields
  - Map nature_of_incident → incident_type enum
  - Map hate_crime_included → incident_severity enum
  - Extract state/city from address block
  - Copy all new fields (motive_cause, title, street etc.)
  - Sync jotform_approval

Stage 2 — AI enrichment (optional, costs money):
  - Read description + source links
  - Fill missing incident_type, bias_motivation, victim_religion etc.
  - Extract named persons into persons table

Usage:
    uv run python pipeline/process.py                    # dry-run
    uv run python pipeline/process.py --fix              # process all unprocessed
    uv run python pipeline/process.py --fix --all        # reprocess all records
    uv run python pipeline/process.py --fix --limit 100  # test on 100
    uv run python pipeline/process.py --fix --ai         # also run AI enrichment
    uv run python pipeline/process.py --fix --ai --budget 5.00
"""

import os
import re
import sys
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
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/process.log'),
    ],
)
log = logging.getLogger(__name__)

DATABASE_URL   = os.environ.get('DATABASE_URL')
ANTHROPIC_KEY  = os.environ.get('ANTHROPIC_API_KEY')

if not DATABASE_URL:
    raise SystemExit('ERROR: DATABASE_URL missing from .env')

# ---------------------------------------------------------------------------
# Enum mappings
# ---------------------------------------------------------------------------

NATURE_TO_INCIDENT_TYPE = {
    'lynching':                     'lynching',
    'mob assault':                  'mob_assault',
    'mob violence':                 'mob_assault',
    'murder':                       'murder',
    'rape':                         'rape',
    'sexual assault':               'sexual_assault',
    'sexual harassment':            'sexual_assault',
    'property destruction':         'property_destruction',
    'property damage':              'property_destruction',
    'arson':                        'arson',
    'demolition':                   'demolition',
    'bulldozer':                    'demolition',
    'eviction':                     'eviction',
    'false fir':                    'false_fir',
    'false case':                   'false_fir',
    'uapa':                         'uapa_detention',
    'nsa':                          'nsa_detention',
    'hate speech':                  'hate_speech',
    'economic boycott':             'economic_boycott',
    'boycott':                      'economic_boycott',
    'forced conversion':            'forced_conversion_allegation',
    'religious site':               'religious_site_destruction',
    'mosque':                       'religious_site_destruction',
    'church':                       'religious_site_destruction',
    'cow vigilant':                 'cow_vigilantism',
    'gau raksha':                   'cow_vigilantism',
    'anti conversion':              'anti_conversion_law_misuse',
    'love jihad':                   'love_jihad_allegation',
    'communal riot':                'communal_riot',
    'riot':                         'communal_riot',
    'targeted arrest':              'targeted_arrest',
    'discrimination':               'other',
    'hate crime':                   'other',  # too generic — AI will refine
}

HATE_CRIME_TO_SEVERITY = {
    'violence causing death':       'death',
    'murder':                       'death',
    'killed':                       'death',
    'death':                        'death',
    'grievous hurt':                'grievous_hurt',
    'serious injury':               'grievous_hurt',
    'physical assault':             'assault',
    'assault':                      'assault',
    'sexual violence':              'sexual_violence',
    'rape':                         'sexual_violence',
    'sexual assault':               'sexual_violence',
    'property demolition':          'property_damage',
    'property damage':              'property_damage',
    'vandalism':                    'property_damage',
    'arson':                        'property_damage',
    'threat':                       'threat',
    'intimidation':                 'threat',
    'verbal harassment':            'threat',
    'psychological':                'psychological_harm',
    'detention':                    'detention',
    'arrest':                       'detention',
    'displacement':                 'displacement',
    'eviction':                     'displacement',
    'hate speech':                  'hate_speech_only',
}

INDIA_STATE_CODES = {
    'AP': 'Andhra Pradesh', 'AR': 'Arunachal Pradesh', 'AS': 'Assam',
    'BR': 'Bihar', 'CG': 'Chhattisgarh', 'GA': 'Goa', 'GJ': 'Gujarat',
    'HR': 'Haryana', 'HP': 'Himachal Pradesh', 'JH': 'Jharkhand',
    'KA': 'Karnataka', 'KL': 'Kerala', 'MP': 'Madhya Pradesh',
    'MH': 'Maharashtra', 'MN': 'Manipur', 'ML': 'Meghalaya',
    'MZ': 'Mizoram', 'NL': 'Nagaland', 'OD': 'Odisha', 'OR': 'Odisha',
    'PB': 'Punjab', 'RJ': 'Rajasthan', 'SK': 'Sikkim', 'TN': 'Tamil Nadu',
    'TS': 'Telangana', 'TR': 'Tripura', 'UP': 'Uttar Pradesh',
    'UK': 'Uttarakhand', 'WB': 'West Bengal', 'DL': 'Delhi',
    'JK': 'Jammu & Kashmir', 'LA': 'Ladakh', 'PY': 'Puducherry',
    'CH': 'Chandigarh', 'AN': 'Andaman & Nicobar',
}

# ---------------------------------------------------------------------------
# Rule-based extractors
# ---------------------------------------------------------------------------

def map_incident_type(nature: str) -> Optional[str]:
    """Map nature_of_incident text → incident_type enum."""
    if not nature:
        return None
    n = nature.strip().lower()
    for key, val in NATURE_TO_INCIDENT_TYPE.items():
        if key in n:
            return val
    return 'other'


def map_severity(hate_crime_included: list) -> Optional[str]:
    """Map hate_crime_included array → most severe incident_severity."""
    if not hate_crime_included:
        return None
    severity_order = [
        'death', 'grievous_hurt', 'sexual_violence', 'assault',
        'property_damage', 'detention', 'displacement',
        'threat', 'psychological_harm', 'hate_speech_only',
    ]
    found = set()
    for item in hate_crime_included:
        item_lower = item.lower()
        for key, sev in HATE_CRIME_TO_SEVERITY.items():
            if key in item_lower:
                found.add(sev)
    for sev in severity_order:
        if sev in found:
            return sev
    return None


def parse_person_json(raw: str) -> list[dict]:
    """Parse victim_details_raw or suspect_details_raw JSON string."""
    if not raw:
        return []
    try:
        if isinstance(raw, str):
            data = json.loads(raw)
        else:
            data = raw
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except Exception:
        pass
    return []


def clean_person_value(val: str) -> Optional[str]:
    """Return None for placeholder values like 'Please select'."""
    if not val:
        return None
    v = str(val).strip()
    if v.lower() in ('please select', 'select', 'unknown', 'unkown',
                     'n/a', 'na', 'none', ''):
        return None
    return v


def extract_victim_fields(victim_details_raw: str) -> dict:
    """Extract structured fields from victim_details_raw JSON."""
    persons = parse_person_json(victim_details_raw)
    if not persons:
        return {}

    # Collect all non-null values across all victims
    religions = []
    castes    = []
    genders   = []

    for p in persons:
        rc = clean_person_value(p.get('Religion/Caste', ''))
        if rc:
            # Split "Hindu - Savarna" into religion and caste
            if ' - ' in rc:
                parts = rc.split(' - ', 1)
                religions.append(parts[0].strip())
                castes.append(parts[1].strip())
            else:
                religions.append(rc)

        gender = clean_person_value(p.get('Gender', ''))
        if gender:
            genders.append(gender)

    result = {}
    if religions:
        result['victim_religion'] = ', '.join(dict.fromkeys(religions))
    if castes:
        result['victim_caste'] = ', '.join(dict.fromkeys(castes))
    if genders:
        result['victim_gender'] = ', '.join(dict.fromkeys(genders))
    return result


def extract_suspect_fields(suspect_details_raw: str) -> dict:
    """Extract structured fields from suspect_details_raw JSON."""
    persons = parse_person_json(suspect_details_raw)
    if not persons:
        return {}

    religions    = []
    orgs         = []
    political    = []

    for p in persons:
        rc = clean_person_value(p.get('Religion/Caste', ''))
        if rc:
            if ' - ' in rc:
                religions.append(rc.split(' - ', 1)[0].strip())
            else:
                religions.append(rc)

        org = clean_person_value(p.get('Organizational Affiliation', ''))
        if org:
            orgs.append(org)

        pol = clean_person_value(p.get('Political Affiliation', ''))
        if pol:
            political.append(pol)

    result = {}
    if religions:
        result['suspect_religion'] = ', '.join(dict.fromkeys(religions))
    if orgs:
        result['suspect_org_affiliation'] = ', '.join(dict.fromkeys(orgs))
    if political:
        result['suspect_political_affiliation'] = ', '.join(dict.fromkeys(political))
    return result


def parse_address_block(address: str) -> dict:
    """Extract city/state/postal_code from JotForm address block."""
    result = {}
    if not address:
        return result
    for line in address.split('\n'):
        line = line.strip()
        if ':' not in line:
            continue
        key, _, val = line.partition(':')
        key = key.strip().lower()
        val = val.strip()
        if not val:
            continue
        if key == 'city':
            result['city'] = val
        elif key == 'state':
            result['state'] = INDIA_STATE_CODES.get(val.upper(), val)
            result['state_code'] = val.upper()[:2]
        elif key in ('postal code', 'postal_code', 'zip', 'pincode'):
            result['postal_code'] = val
    return result


def normalise_state(val: str) -> tuple[Optional[str], Optional[str]]:
    """Expand state code abbreviation if needed. Returns (state, state_code)."""
    if not val:
        return None, None
    val = val.strip()
    if len(val) <= 3:
        full = INDIA_STATE_CODES.get(val.upper())
        if full:
            return full, val.upper()[:2]
    return val, None


def to_bool(val) -> Optional[bool]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ('yes', 'true', '1', 'y', 'filed'):
        return True
    if s in ('no', 'false', '0', 'n', 'not filed'):
        return False
    return None


def to_int(val) -> Optional[int]:
    if val is None:
        return None
    # Handle ranges like "1-5" → take lower bound
    s = str(val).strip()
    m = re.match(r'^(\d+)', s)
    if m:
        return int(m.group(1))
    return None


def normalise_fir_status(val) -> str:
    if not val:
        return 'unknown'
    s = str(val).strip().lower()
    if any(x in s for x in ('yes', 'filed', 'true')):
        return 'filed'
    if any(x in s for x in ('no', 'not', 'false')):
        return 'not_filed'
    return 'unknown'


def normalise_police_role(val) -> str:
    if not val:
        return 'unknown'
    s = str(val).strip().lower()
    mapping = {
        'complicit':     'complicit',
        'failed':        'failed_to_act',
        'unatisfactory': 'failed_to_act',
        'unsatisfactory':'failed_to_act',
        'delayed':       'delayed_action',
        'acted':         'acted_appropriately',
        'appropriate':   'acted_appropriately',
        'cross fir':     'filed_cross_fir',
    }
    for key, norm in mapping.items():
        if key in s:
            return norm
    return 'unknown'


# ---------------------------------------------------------------------------
# AI enrichment (Stage 2 — optional)
# ---------------------------------------------------------------------------

AI_ENRICH_PROMPT = """You are enriching a hate crime incident record for a human rights database.

INCIDENT:
Date: {date}
Location: {location}
Description: {description}
Nature: {nature}
Source URLs: {urls}

Extract ONLY what is clearly stated or strongly implied. Return JSON:
{{
  "incident_type": null,
  "incident_severity": null,
  "bias_motivation": [],
  "victim_religion": null,
  "victim_caste": null,
  "perpetrator_type": null,
  "suspect_org_affiliation": null,
  "named_victims": [],
  "named_perpetrators": [],
  "confidence": 0.0,
  "notes": ""
}}

incident_type options: lynching, mob_assault, murder, rape, sexual_assault,
  property_destruction, arson, demolition, eviction, false_fir, uapa_detention,
  nsa_detention, hate_speech, economic_boycott, forced_conversion_allegation,
  religious_site_destruction, cow_vigilantism, anti_conversion_law_misuse,
  love_jihad_allegation, communal_riot, targeted_arrest, other

incident_severity options: death, grievous_hurt, assault, sexual_violence,
  property_damage, threat, psychological_harm, detention, displacement, hate_speech_only

bias_motivation options: religion, caste, ethnicity, gender, sexual_orientation,
  disability, political, language, regional_identity, dietary_practice,
  interfaith_relationship, occupation, other

perpetrator_type options: state_actor, non_state_organised, non_state_individual,
  mob, police, paramilitary, media, unknown

Return ONLY the JSON object, nothing else."""


# ---------------------------------------------------------------------------
# Model registry — pricing per million tokens (verify at provider websites)
# ---------------------------------------------------------------------------
MODEL_REGISTRY = {
    # Anthropic
    'claude-haiku-4-5':      {'provider': 'anthropic', 'input': 0.80,  'output': 4.00,  'label': 'Claude Haiku 4.5 (cheapest, good)'},
    'claude-sonnet-4-6':     {'provider': 'anthropic', 'input': 3.00,  'output': 15.00, 'label': 'Claude Sonnet 4.6 (best quality)'},
    # Google
    'gemini-1.5-flash':      {'provider': 'google',    'input': 0.075, 'output': 0.30,  'label': 'Gemini 1.5 Flash (cheapest overall)'},
    'gemini-1.5-pro':        {'provider': 'google',    'input': 1.25,  'output': 5.00,  'label': 'Gemini 1.5 Pro (good India context)'},
    # OpenAI
    'gpt-4o-mini':           {'provider': 'openai',    'input': 0.15,  'output': 0.60,  'label': 'GPT-4o Mini (cheap, decent)'},
    'gpt-4o':                {'provider': 'openai',    'input': 2.50,  'output': 10.00, 'label': 'GPT-4o (good quality)'},
    # DeepSeek
    'deepseek-chat':         {'provider': 'deepseek',  'input': 0.27,  'output': 1.10,  'label': 'DeepSeek V3 (cheapest, weaker India context)'},
}

DEFAULT_MODEL = 'claude-haiku-4-5'


def calc_cost(inp: int, out: int, model: str = DEFAULT_MODEL) -> float:
    m = MODEL_REGISTRY.get(model, MODEL_REGISTRY[DEFAULT_MODEL])
    return (inp / 1_000_000) * m['input'] + (out / 1_000_000) * m['output']


def estimate_total_cost(num_records: int, model: str = DEFAULT_MODEL,
                        avg_input: int = 500, avg_output: int = 200) -> float:
    return calc_cost(avg_input * num_records, avg_output * num_records, model)

def ai_enrich_record(client, raw: dict, model: str = DEFAULT_MODEL) -> Optional[dict]:
    """Use AI to enrich a record. Returns enrichment dict or None."""
    try:
        prompt = AI_ENRICH_PROMPT.format(
            date        = raw.get('incident_date', 'unknown'),
            location    = f"{raw.get('city','')}, {raw.get('state','')}".strip(', '),
            description = (raw.get('description') or '')[:500],
            nature      = raw.get('nature_of_incident', ''),
            urls        = (raw.get('website_urls_raw') or '')[:300],
        )

        provider = MODEL_REGISTRY.get(model, {}).get('provider', 'anthropic')

        if provider == 'anthropic':
            import anthropic as _anthropic
            response = client.messages.create(
                model      = model,
                max_tokens = 500,
                messages   = [{'role': 'user', 'content': prompt}],
            )
            text = response.content[0].text.strip()
            inp  = response.usage.input_tokens
            out  = response.usage.output_tokens

        elif provider == 'openai':
            response = client.chat.completions.create(
                model    = model,
                messages = [{'role': 'user', 'content': prompt}],
                max_tokens = 500,
            )
            text = response.choices[0].message.content.strip()
            inp  = response.usage.prompt_tokens
            out  = response.usage.completion_tokens

        elif provider == 'google':
            response = client.generate_content(prompt)
            text = response.text.strip()
            inp  = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out  = getattr(response.usage_metadata, 'candidates_token_count', 0)

        elif provider == 'deepseek':
            # DeepSeek uses OpenAI-compatible API
            response = client.chat.completions.create(
                model    = model,
                messages = [{'role': 'user', 'content': prompt}],
                max_tokens = 500,
            )
            text = response.choices[0].message.content.strip()
            inp  = response.usage.prompt_tokens
            out  = response.usage.completion_tokens
        else:
            raise ValueError(f'Unknown provider: {provider}')

        start = text.find('{')
        end   = text.rfind('}') + 1
        if start >= 0 and end > start:
            result = json.loads(text[start:end])
            result['_input_tokens']  = inp
            result['_output_tokens'] = out
            return result

    except Exception as e:
        log.warning('AI enrichment failed for raw_id %s: %s',
                    raw.get('raw_id'), e)
    return None


# ---------------------------------------------------------------------------
# Build incidents row from raw record
# ---------------------------------------------------------------------------

def build_incidents_row(raw: dict, ai_result: Optional[dict] = None,
                        ai_model: str = 'unknown') -> dict:
    """
    Transform a raw_incidents row into an incidents row.
    Protected fields (already set by geo_enrichment, ai_verify etc.)
    are only filled if currently NULL — using COALESCE in the upsert.
    """
    row = {}

    # ── Identifiers ──────────────────────────────────────────────────────
    row['jotform_submission_id'] = raw.get('jotform_submission_id')
    row['jotform_approval']      = raw.get('jotform_approval')

    # ── Core fields — always copy from raw ───────────────────────────────
    for field in (
        'title', 'description', 'nature_of_incident',
        'hate_crime_included', 'motive_cause', 'other_motive', 'tags',
        'incident_date', 'type_of_venue', 'other_venue_type',
        'type_of_discrimination', 'number_of_suspects', 'hate_speech_made_by',
        'fir_filed_against', 'police_station', 'investigating_agency',
        'other_investigating_agency', 'investigating_officer',
        'case_current_status', 'other_current_status',
        'cross_fir_filed_against', 'state_government_party', 'other_state_party',
        'source_of_information', 'other_source', 'source_app',
        'image_source', 'video_source', 'images_by', 'video_by',
        'victim_details_raw', 'suspect_details_raw', 'geolocation_raw',
        'submission_ip', 'postal_code', 'street', 'house_number',
    ):
        val = raw.get(field)
        if val is not None:
            row[field] = val

    # ── FIR fields ────────────────────────────────────────────────────────
    row['fir_status']         = normalise_fir_status(raw.get('fir_filed_raw'))
    row['fir_filed_date']     = raw.get('fir_filed_date')
    row['fir_charges']        = raw.get('fir_charges_raw')
    row['cross_fir_filed']    = to_bool(raw.get('cross_fir_filed_raw'))
    row['cross_fir_filed_date'] = raw.get('cross_fir_filed_date')
    row['cross_fir_charges']  = raw.get('cross_fir_charges_raw')
    row['police_role']        = normalise_police_role(raw.get('police_role_raw'))

    # ── Victim counts ─────────────────────────────────────────────────────
    row['casualties']     = to_int(raw.get('casualties_raw'))
    row['injured']        = to_int(raw.get('injured_raw'))
    row['harassed']       = to_bool(raw.get('harassed_raw'))
    row['displaced']      = to_bool(raw.get('displaced_raw'))
    row['property_damage']= to_bool(raw.get('property_damage_raw'))
    row['online_harassment'] = to_bool(raw.get('online_harassment_raw'))

    # ── Location — extract from address block if not set ─────────────────
    addr_fields = parse_address_block(raw.get('address_raw', ''))
    row['address'] = raw.get('address_raw')
    if addr_fields.get('city') and not raw.get('city'):
        row['city'] = addr_fields['city']
    elif raw.get('city'):
        row['city'] = raw['city']

    if addr_fields.get('state'):
        row['state']      = addr_fields['state']
        row['state_code'] = addr_fields.get('state_code')
    elif raw.get('state'):
        state, code = normalise_state(raw['state'])
        row['state']      = state
        row['state_code'] = code

    if addr_fields.get('postal_code') and not row.get('postal_code'):
        row['postal_code'] = addr_fields['postal_code']

    # ── Rule-based classification ─────────────────────────────────────────
    # Only fill if not already set (COALESCE in upsert handles existing)
    row['incident_type']     = map_incident_type(raw.get('nature_of_incident'))
    row['incident_severity'] = map_severity(raw.get('hate_crime_included') or [])

    # ── Structured victim/suspect fields ──────────────────────────────────
    victim_fields  = extract_victim_fields(raw.get('victim_details_raw'))
    suspect_fields = extract_suspect_fields(raw.get('suspect_details_raw'))
    row.update(victim_fields)
    row.update(suspect_fields)

    # ── AI enrichment (overrides rules where AI is more confident) ────────
    if ai_result and ai_result.get('confidence', 0) >= 0.7:
        ai_map = {
            'incident_type':             'incident_type',
            'incident_severity':         'incident_severity',
            'bias_motivation':           'bias_motivation',
            'victim_religion':           'victim_religion',
            'victim_caste':              'victim_caste',
            'perpetrator_type':          'perpetrator_type',
            'suspect_org_affiliation':   'suspect_org_affiliation',
        }
        for ai_key, db_key in ai_map.items():
            val = ai_result.get(ai_key)
            if val and val not in (None, [], ''):
                row[db_key] = val

    # ── Append verification_log entry ─────────────────────────────────────
    log_entry = {
        'ts':     datetime.now(timezone.utc).isoformat(),
        'action': 'processed',
        'by':     'process.py',
        'stage':  'rule_based' if not ai_result else 'ai_enriched',
    }
    if ai_result:
        log_entry['ai_confidence'] = ai_result.get('confidence', 0)
        log_entry['ai_model']      = ai_model
        log_entry['ai_notes']      = ai_result.get('notes', '')

    row['_verification_log_entry'] = log_entry  # handled specially in upsert

    # ── Provenance ────────────────────────────────────────────────────────
    # Map raw source_type to valid data_source_enum values
    source_type_map = {
        'jotform_api': 'jotform',
        'jotform_csv': 'jotform',
        'manual':      'manual',
        'ai_claude':   'ai_extracted',
        'ai_gemini':   'ai_extracted',
        'ai_deepseek': 'ai_extracted',
    }
    raw_source = raw.get('source_type', 'jotform_api')
    row['data_source'] = source_type_map.get(raw_source, 'jotform')

    # Remove None values
    row = {k: v for k, v in row.items() if v is not None}

    return row


# ---------------------------------------------------------------------------
# Enum cast map — used in upsert SQL generation
# ---------------------------------------------------------------------------
ENUM_CASTS = {
    'incident_type':      'incident_type_enum',
    'incident_severity':  'incident_severity_enum',
    'bias_motivation':    'bias_motivation_enum[]',
    'data_source':        'data_source_enum',
    'verification_status':'verification_status_enum',
    'reliability_level':  'reliability_level_enum',
    'fir_status':         'fir_status_enum',
    'police_role':        'police_role_enum',
    'perpetrator_type':   'perpetrator_type_enum',
    'approval_status':    'approval_status_enum',
}

# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def fetch_raw_records(conn, limit: int, offset: int,
                      reprocess_all: bool) -> list[dict]:
    """Fetch raw records to process."""
    condition = 'TRUE' if reprocess_all else 'r.processed = FALSE'
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT r.*
            FROM raw_incidents r
            WHERE {condition}
            AND r.jotform_submission_id IS NOT NULL
            ORDER BY r.id
            LIMIT %s OFFSET %s
        """, (limit, offset))
        return [dict(r) for r in cur.fetchall()]


def count_raw_records(conn, reprocess_all: bool) -> int:
    condition = 'TRUE' if reprocess_all else 'processed = FALSE'
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM raw_incidents WHERE {condition}')
        return cur.fetchone()[0]


def upsert_incident(conn, row: dict) -> tuple[bool, bool]:
    """
    Upsert one incident row.

    Protected columns (set by geo_enrichment, ai_verify, manual review)
    use COALESCE — only filled if currently NULL.
    """
    log_entry = row.pop('_verification_log_entry', None)

    # Columns that should never be overwritten by process.py
    PROTECTED = {
        'latitude', 'longitude', 'district',        # geo_enrichment owns these
        'verification_status', 'reliability_level',  # validate.py / ai_verify.py
        'review_notes', 'published', 'sensitive',
        'retracted', 'retraction_reason',
        'verified_by', 'verified_at',
        'ai_provider', 'ai_model',
        'entered_by', 'reviewed_by',
    }

    cols = list(row.keys())
    vals = [row[c] for c in cols]

    update_cols     = [c for c in cols if c != 'jotform_submission_id'
                       and c not in PROTECTED]
    protected_present = [c for c in cols if c in PROTECTED]

    # Build UPDATE SET with enum casts where needed
    def excluded_ref(c):
        if c in ENUM_CASTS:
            return f'EXCLUDED.{c}::{ENUM_CASTS[c]}'
        return f'EXCLUDED.{c}'

    update_set = ', '.join(f'{c} = {excluded_ref(c)}' for c in update_cols)
    if protected_present:
        coalesce = ', '.join(
            f'{c} = COALESCE(incidents.{c}, {excluded_ref(c)})'
            for c in protected_present
        )
        update_set = f'{update_set}, {coalesce}' if update_set else coalesce

    ph_list = []
    for c in cols:
        if c in ENUM_CASTS:
            ph_list.append(f'%s::{ENUM_CASTS[c]}')
        else:
            ph_list.append('%s')
    ph      = ', '.join(ph_list)
    col_str = ', '.join(cols)

    sql = f"""
        INSERT INTO incidents ({col_str})
        VALUES ({ph})
        ON CONFLICT (jotform_submission_id)
        DO UPDATE SET {update_set},
                      updated_at = NOW()
        RETURNING id, (xmax = 0) AS is_insert
    """

    with conn.cursor() as cur:
        cur.execute(sql, vals)
        result  = cur.fetchone()
        inc_id  = result[0]
        is_new  = result[1]

    # Append to verification_log (never overwrite)
    if log_entry and inc_id:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE incidents
                SET verification_log = verification_log || %s::jsonb
                WHERE id = %s
                AND NOT (verification_log @> %s::jsonb)
            """, (
                json.dumps([log_entry]),
                inc_id,
                json.dumps([{'action': log_entry['action'],
                             'stage':  log_entry.get('stage', '')}]),
            ))

    return is_new, not is_new


def mark_raw_processed(conn, raw_ids: list[int], incident_id_map: dict):
    """Mark raw records as processed and link to incident IDs."""
    if not raw_ids:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE raw_incidents
            SET processed    = TRUE,
                processed_at = NOW(),
                incident_id  = %s
            WHERE id = %s
            """,
            [(incident_id_map.get(rid), rid) for rid in raw_ids],
            page_size=500,
        )


def get_incident_ids(conn, jotform_ids: list[str]) -> dict[str, int]:
    if not jotform_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            'SELECT jotform_submission_id, id FROM incidents '
            'WHERE jotform_submission_id = ANY(%s)',
            (jotform_ids,)
        )
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Cost tracking for AI enrichment
# ---------------------------------------------------------------------------



def log_ai_cost(conn, incident_id: int, inp: int, out: int,
                cost: float, model: str = DEFAULT_MODEL):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_costs
                    (incident_id, script, model, input_tokens,
                     output_tokens, cost_usd, created_at)
                VALUES (%s, 'process_ai', %s, %s, %s, %s, NOW())
            """, (incident_id, model, inp, out, cost))
    except Exception as e:
        log.warning('Could not log AI cost: %s', e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool, limit: int, reprocess_all: bool,
        use_ai: bool, budget: Optional[float], ai_model: str = DEFAULT_MODEL):

    mode = 'DRY RUN' if dry_run else 'LIVE'
    log.info('=== process.py started (%s) ===', mode)

    conn  = psycopg2.connect(DATABASE_URL)
    total = count_raw_records(conn, reprocess_all)
    log.info('Raw records to process: %d', total)

    if use_ai:
        model_info = MODEL_REGISTRY.get(ai_model, {})
        provider   = model_info.get('provider', 'anthropic')
        label      = model_info.get('label', ai_model)
        est_cost   = estimate_total_cost(min(limit, total), ai_model)

        log.info('AI enrichment enabled: %s', label)
        log.info('Estimated cost for %d records: $%.2f', min(limit, total), est_cost)

        # Create correct client for provider
        if provider == 'anthropic':
            if not ANTHROPIC_KEY:
                raise SystemExit('ERROR: ANTHROPIC_API_KEY missing from .env')
            import anthropic
            ai_client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        elif provider == 'openai':
            api_key = os.environ.get('OPENAI_API_KEY')
            if not api_key:
                raise SystemExit('ERROR: OPENAI_API_KEY missing from .env')
            from openai import OpenAI
            ai_client = OpenAI(api_key=api_key)
        elif provider == 'google':
            api_key = os.environ.get('GOOGLE_API_KEY')
            if not api_key:
                raise SystemExit('ERROR: GOOGLE_API_KEY missing from .env')
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            ai_client = genai.GenerativeModel(ai_model)
        elif provider == 'deepseek':
            api_key = os.environ.get('DEEPSEEK_API_KEY')
            if not api_key:
                raise SystemExit('ERROR: DEEPSEEK_API_KEY missing from .env')
            from openai import OpenAI
            ai_client = OpenAI(
                api_key=api_key,
                base_url='https://api.deepseek.com'
            )
        else:
            raise SystemExit(f'ERROR: Unknown provider: {provider}')
    else:
        ai_client = None

    cap        = min(limit, total)
    offset     = 0
    counts     = {'new': 0, 'updated': 0, 'errors': 0, 'ai_enriched': 0}
    ai_cost    = 0.0
    budget_hit = False

    while offset < cap:
        batch_size = min(100, cap - offset)
        raw_batch  = fetch_raw_records(conn, batch_size, offset, reprocess_all)
        if not raw_batch:
            break

        raw_ids         = []
        incident_id_map = {}

        for raw in raw_batch:
            try:
                # Snapshot key fields before processing (for comparison report)
                before_snap = {}
                if use_ai:
                    sub_id = raw.get('jotform_submission_id')
                    if sub_id:
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as _cur:
                            _cur.execute(
                                'SELECT incident_type, incident_severity, '
                                'bias_motivation, victim_religion, '
                                'suspect_org_affiliation, perpetrator_type '
                                'FROM incidents WHERE jotform_submission_id = %s',
                                (sub_id,)
                            )
                            _snap = _cur.fetchone()
                            if _snap:
                                before_snap = dict(_snap)

                # AI enrichment (optional)
                ai_result = None
                if use_ai and ai_client and not budget_hit:
                    if budget and ai_cost >= budget:
                        log.warning('AI budget $%.2f reached', budget)
                        budget_hit = True
                    else:
                        ai_result = ai_enrich_record(ai_client, raw, model=ai_model)
                        if ai_result:
                            inp = ai_result.pop('_input_tokens', 0)
                            out = ai_result.pop('_output_tokens', 0)
                            cost = calc_cost(inp, out)
                            ai_cost += cost
                            counts['ai_enriched'] += 1

                # Build incidents row
                row = build_incidents_row(raw, ai_result, ai_model=ai_model)

                if dry_run:
                    raw_ids.append(raw['id'])
                    continue

                # Upsert to incidents
                is_new, is_updated = upsert_incident(conn, row)

                if is_new:
                    counts['new'] += 1
                else:
                    counts['updated'] += 1

                # Before/after comparison log
                if use_ai and ai_result and before_snap:
                    conf = ai_result.get('confidence', 0)
                    log.info('  AI (conf %.0f%%) changes for %s:',
                             conf * 100, raw.get('jotform_submission_id','?')[:20])
                    check_fields = [
                        ('incident_type',       row.get('incident_type')),
                        ('incident_severity',   row.get('incident_severity')),
                        ('bias_motivation',     row.get('bias_motivation')),
                        ('victim_religion',     row.get('victim_religion')),
                        ('suspect_org',         row.get('suspect_org_affiliation')),
                        ('perpetrator_type',    row.get('perpetrator_type')),
                    ]
                    before_map = {
                        'incident_type':     before_snap.get('incident_type'),
                        'incident_severity': before_snap.get('incident_severity'),
                        'bias_motivation':   before_snap.get('bias_motivation'),
                        'victim_religion':   before_snap.get('victim_religion'),
                        'suspect_org':       before_snap.get('suspect_org_affiliation'),
                        'perpetrator_type':  before_snap.get('perpetrator_type'),
                    }
                    any_change = False
                    for fname, new_val in check_fields:
                        old_val = before_map.get(fname)
                        if new_val and new_val != old_val:
                            log.info('    + %-22s None → %s', fname, repr(new_val))
                            any_change = True
                    if not any_change:
                        log.info('    (no new fields added by AI)'  )

                # Track raw_id → incident_id mapping
                raw_ids.append(raw['id'])

            except Exception as e:
                conn.rollback()
                counts['errors'] += 1
                log.error('ERROR processing raw_id %s: %s', raw.get('id'), e)

        if not dry_run:
            # Get incident IDs for this batch
            jotform_ids = [r['jotform_submission_id'] for r in raw_batch
                           if r.get('jotform_submission_id')]
            id_map = get_incident_ids(conn, jotform_ids)

            # Map raw_id → incident_id
            for raw in raw_batch:
                sub_id = raw.get('jotform_submission_id')
                if sub_id and sub_id in id_map:
                    incident_id_map[raw['id']] = id_map[sub_id]

            # Log AI costs
            if use_ai:
                for raw in raw_batch:
                    sub_id = raw.get('jotform_submission_id')
                    if sub_id and sub_id in id_map:
                        log_ai_cost(conn, id_map[sub_id], 0, 0, 0, ai_model)

            # Mark raw records as processed
            mark_raw_processed(conn, raw_ids, incident_id_map)
            conn.commit()

        log.info(
            'Batch %d–%d: +%d new, ~%d updated%s',
            offset + 1, offset + len(raw_batch),
            counts['new'], counts['updated'],
            f', AI: {counts["ai_enriched"]}' if use_ai else '',
        )
        offset += len(raw_batch)

    print()
    print('=' * 55)
    print(f'  process.py — Run Summary ({mode})')
    print('=' * 55)
    print(f'  Total raw records:    {total:,}')
    print(f'  Processed:            {offset:,}')
    print(f'  New incidents:        {counts["new"]:,}')
    print(f'  Updated incidents:    {counts["updated"]:,}')
    print(f'  Errors:               {counts["errors"]}')
    if use_ai:
        model_info = MODEL_REGISTRY.get(ai_model, {})
        print(f'  AI model:             {model_info.get("label", ai_model)}')
        print(f'  AI enriched:          {counts["ai_enriched"]:,}')
        print(f'  AI cost this run:     ${ai_cost:.4f}')
        if budget_hit:
            print(f'  ⚠ Budget limit hit:  ${budget:.2f}')
    print('=' * 55)
    if dry_run:
        print('  Run with --fix to apply changes')
    print()

    conn.close()
    log.info('=== process.py complete ===')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='HateWatch raw_incidents → incidents enrichment',
        epilog='Default: dry-run. Use --fix to write to DB.'
    )
    parser.add_argument('--fix',    action='store_true',
                        help='Write to DB (default: dry-run)')
    parser.add_argument('--all',    action='store_true',
                        help='Reprocess all records, not just unprocessed')
    parser.add_argument('--limit',  type=int, default=999999,
                        help='Max records to process')
    parser.add_argument('--ai',     action='store_true',
                        help='Enable AI enrichment for missing fields')
    parser.add_argument('--model',  type=str, default=DEFAULT_MODEL,
                        help=f'AI model to use (default: {DEFAULT_MODEL}). '
                             f'Use --list-models to see options.')
    parser.add_argument('--budget', type=float, default=None,
                        help='Stop AI enrichment if cost exceeds this USD amount')
    parser.add_argument('--list-models', action='store_true', dest='list_models',
                        help='Show available AI models with pricing and exit')
    parser.add_argument('--estimate', action='store_true',
                        help='Show cost estimate for --model without running')
    args = parser.parse_args()

    if args.list_models:
        print()
        print('=' * 65)
        print('  Available AI models for --model flag')
        print('=' * 65)
        print(f'  {"Model":<25} {"Provider":<12} {"$/1M in":>8} {"$/1M out":>9}  Label')
        print(f'  {"-"*25} {"-"*12} {"-"*8} {"-"*9}  {"-"*30}')
        for name, info in MODEL_REGISTRY.items():
            marker = ' ← default' if name == DEFAULT_MODEL else ''
            print(f'  {name:<25} {info["provider"]:<12} '
                  f'${info["input"]:>7.3f} ${info["output"]:>8.3f}  '
                  f'{info["label"]}{marker}')
        print()
        print('  Usage: uv run python pipeline/process.py --fix --ai --model gemini-1.5-flash')
        print('=' * 65)
        print()
        import sys; sys.exit(0)

    if args.estimate and args.ai:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv()
        conn2 = psycopg2.connect(os.environ['DATABASE_URL'])
        total2 = count_raw_records(conn2, args.all)
        conn2.close()
        cap = min(args.limit, total2)
        est = estimate_total_cost(cap, args.model)
        info = MODEL_REGISTRY.get(args.model, {})
        print()
        print(f'  Model:    {info.get("label", args.model)}')
        print(f'  Records:  {cap:,}')
        print(f'  Est cost: ${est:.4f}')
        print()
        import sys; sys.exit(0)

    run(
        dry_run      = not args.fix,
        limit        = args.limit,
        reprocess_all= args.all,
        use_ai       = args.ai,
        budget       = args.budget,
        ai_model     = args.model,
    )