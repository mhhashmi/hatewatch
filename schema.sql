-- =============================================================================
-- HateWatch — India Hate Speech & Hate Crime Observatory
-- Database Schema v1.0
-- =============================================================================
-- 17 tables covering:
--   - Incidents, persons, organisations
--   - Legal actions, case timelines, detentions
--   - State actions (bulldozer, UAPA, false FIRs)
--   - Properties, speeches, media coverage
--   - AI staging, sources, sync log
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ENUMS
-- ---------------------------------------------------------------------------

CREATE TYPE incident_type_enum AS ENUM (
    'lynching',
    'mob_assault',
    'murder',
    'rape',
    'sexual_assault',
    'property_destruction',
    'arson',
    'demolition',
    'eviction',
    'false_fir',
    'uapa_detention',
    'nsa_detention',
    'hate_speech',
    'economic_boycott',
    'forced_conversion_allegation',
    'religious_site_destruction',
    'cow_vigilantism',
    'anti_conversion_law_misuse',
    'love_jihad_allegation',
    'communal_riot',
    'targeted_arrest',
    'other'
);

CREATE TYPE incident_severity_enum AS ENUM (
    'death',
    'grievous_hurt',
    'assault',
    'sexual_violence',
    'property_damage',
    'threat',
    'psychological_harm',
    'detention',
    'displacement',
    'hate_speech_only'
);

CREATE TYPE bias_motivation_enum AS ENUM (
    'religion',
    'caste',
    'ethnicity',
    'gender',
    'sexual_orientation',
    'disability',
    'political',
    'language',
    'regional_identity',
    'dietary_practice',
    'interfaith_relationship',
    'occupation',
    'other'
);

CREATE TYPE verification_status_enum AS ENUM (
    'pending',
    'in_review',
    'needs_sources',
    'verified',
    'published',
    'retracted'
);

CREATE TYPE reliability_level_enum AS ENUM ('1', '2', '3');

CREATE TYPE data_source_enum AS ENUM (
    'jotform',
    'ai_extracted',
    'manual'
);

CREATE TYPE person_type_enum AS ENUM (
    'victim',
    'perpetrator',
    'accused',
    'witness',
    'police_officer',
    'govt_official',
    'politician',
    'judge',
    'lawyer',
    'journalist',
    'media_anchor',
    'activist',
    'religious_leader',
    'other'
);

CREATE TYPE perpetrator_type_enum AS ENUM (
    'state_actor',
    'non_state_organised',
    'non_state_individual',
    'mob',
    'police',
    'paramilitary',
    'media',
    'unknown'
);

CREATE TYPE org_type_enum AS ENUM (
    'political_party',
    'religious_group',
    'govt_body',
    'police_force',
    'court',
    'media_outlet',
    'ngo',
    'paramilitary',
    'other'
);

CREATE TYPE incident_person_role_enum AS ENUM (
    'victim',
    'perpetrator',
    'accused',
    'witness',
    'official',
    'lawyer',
    'judge',
    'other'
);

CREATE TYPE legal_action_type_enum AS ENUM (
    'fir',
    'cross_fir',
    'arrest',
    'bail_application',
    'chargesheet',
    'hearing',
    'stay_order',
    'judgment',
    'acquittal',
    'conviction',
    'appeal',
    'pil',
    'other'
);

CREATE TYPE court_level_enum AS ENUM (
    'magistrate',
    'sessions',
    'high_court',
    'supreme_court',
    'tribunal',
    'other'
);

CREATE TYPE case_event_type_enum AS ENUM (
    'fir_filed',
    'arrest',
    'bail_hearing',
    'bail_denied',
    'bail_granted',
    'chargesheet_filed',
    'chargesheet_not_filed',
    'hearing',
    'stay_order',
    'acquittal',
    'conviction',
    'appeal_filed',
    'hc_reversal',
    'sc_reversal',
    'case_closed',
    'other'
);

CREATE TYPE detention_type_enum AS ENUM (
    'pre_trial',
    'uapa',
    'nsa',
    'psa',
    'misa',
    'other'
);

CREATE TYPE state_action_type_enum AS ENUM (
    'demolition',
    'property_seizure',
    'false_fir',
    'uapa_charge',
    'nsa_detention',
    'relative_arrest',
    'eviction',
    'economic_boycott',
    'travel_ban',
    'passport_seizure',
    'other'
);

CREATE TYPE property_type_enum AS ENUM (
    'residential_home',
    'mosque',
    'church',
    'madrasa',
    'dargah_shrine',
    'gurudwara',
    'commercial',
    'agricultural_land',
    'other'
);

CREATE TYPE speech_platform_enum AS ENUM (
    'tv_broadcast',
    'public_rally',
    'social_media',
    'parliament',
    'state_assembly',
    'religious_gathering',
    'press_conference',
    'online_video',
    'print',
    'other'
);

CREATE TYPE media_coverage_type_enum AS ENUM (
    'incitement',
    'false_narrative',
    'victim_blaming',
    'suppression',
    'accurate_reporting',
    'misleading',
    'other'
);

CREATE TYPE source_type_enum AS ENUM (
    'news_article',
    'court_document',
    'official_record',
    'victim_statement',
    'witness_statement',
    'ngo_report',
    'social_media',
    'video',
    'photo',
    'government_data',
    'other'
);

CREATE TYPE relationship_type_enum AS ENUM (
    'spouse',
    'parent',
    'child',
    'sibling',
    'relative',
    'associate',
    'co_accused',
    'lawyer',
    'activist',
    'other'
);

CREATE TYPE fir_status_enum AS ENUM (
    'filed',
    'not_filed',
    'unknown'
);

CREATE TYPE police_role_enum AS ENUM (
    'complicit',
    'failed_to_act',
    'delayed_action',
    'acted_appropriately',
    'filed_cross_fir',
    'unknown'
);

CREATE TYPE approval_status_enum AS ENUM (
    'in_review',
    'approved',
    'rejected',
    'pending',
    'needs_more_info'
);

CREATE TYPE ai_review_status_enum AS ENUM (
    'pending',
    'assigned',
    'approved',
    'rejected',
    'merged'
);

-- ---------------------------------------------------------------------------
-- PROVENANCE COLUMNS — shared macro (applied manually to each table)
-- These columns appear on every data table:
--   data_source, ai_provider, ai_model, entered_by, reviewed_by,
--   review_notes, verification_status, reliability_level,
--   published, sensitive, retracted, retraction_reason,
--   source_url, created_at, updated_at, deleted_at
-- ---------------------------------------------------------------------------


-- =============================================================================
-- TABLE 1: organisations
-- Must come before persons (persons.org_id → organisations.id)
-- =============================================================================
CREATE TABLE organisations (
    id                  BIGSERIAL PRIMARY KEY,

    -- Identity
    name                TEXT NOT NULL,
    name_original       TEXT,                          -- Hindi/Urdu/regional script
    aliases             TEXT[],                        -- alternate names
    org_type            org_type_enum NOT NULL,
    description         TEXT,

    -- Geography
    state               TEXT,
    state_code          CHAR(2),                       -- NCRB 2-digit state code
    national_reach      BOOLEAN DEFAULT FALSE,

    -- Provenance
    data_source         data_source_enum DEFAULT 'manual',
    ai_provider         TEXT,
    ai_model            TEXT,
    entered_by          TEXT,
    reviewed_by         TEXT,
    review_notes        TEXT,
    verification_status verification_status_enum DEFAULT 'pending',
    reliability_level   reliability_level_enum DEFAULT '1',
    published           BOOLEAN DEFAULT FALSE,
    sensitive           BOOLEAN DEFAULT FALSE,
    retracted           BOOLEAN DEFAULT FALSE,
    retraction_reason   TEXT,
    source_url          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 2: persons
-- =============================================================================
CREATE TABLE persons (
    id                      BIGSERIAL PRIMARY KEY,

    -- Identity
    full_name               TEXT NOT NULL,
    name_original           TEXT,                      -- Devanagari / Arabic script
    name_transliterated     TEXT,                      -- Roman transliteration
    aliases                 TEXT[],
    person_type             person_type_enum,
    perpetrator_type        perpetrator_type_enum,

    -- Demographics
    gender                  TEXT,
    age_group               TEXT,                      -- child/adult/elderly
    religion                TEXT,
    caste                   TEXT,
    ethnicity               TEXT,
    nationality             TEXT DEFAULT 'Indian',
    disability              BOOLEAN,

    -- Affiliation
    org_id                  BIGINT REFERENCES organisations(id) ON DELETE SET NULL,
    political_affiliation   TEXT,
    designation             TEXT,                      -- MLA, SP, Anchor etc.
    state                   TEXT,

    -- Provenance
    data_source             data_source_enum DEFAULT 'manual',
    ai_provider             TEXT,
    ai_model                TEXT,
    entered_by              TEXT,
    reviewed_by             TEXT,
    review_notes            TEXT,
    verification_status     verification_status_enum DEFAULT 'pending',
    reliability_level       reliability_level_enum DEFAULT '1',
    published               BOOLEAN DEFAULT FALSE,
    sensitive               BOOLEAN DEFAULT FALSE,
    retracted               BOOLEAN DEFAULT FALSE,
    retraction_reason       TEXT,
    source_url              TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    deleted_at              TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 3: incidents
-- =============================================================================
CREATE TABLE incidents (
    id                          BIGSERIAL PRIMARY KEY,

    -- JotForm reference
    jotform_submission_id       TEXT UNIQUE,

    -- What happened
    incident_type               incident_type_enum,
    incident_severity           incident_severity_enum,
    bias_motivation             bias_motivation_enum[],
    nature_of_incident          TEXT,
    description                 TEXT,
    tags                        TEXT[],

    -- When
    incident_date               DATE,
    incident_date_approx        BOOLEAN DEFAULT FALSE,  -- true if date is estimated
    incident_time               TIME,

    -- Where
    address                     TEXT,
    city                        TEXT,
    district                    TEXT,
    state                       TEXT,
    state_code                  CHAR(2),               -- NCRB state code
    postal_code                 TEXT,
    latitude                    NUMERIC(9,6),
    longitude                   NUMERIC(9,6),
    type_of_venue               TEXT[],

    -- Victim summary (structured counts — detail in incident_persons)
    casualties                  INTEGER,
    injured                     INTEGER,
    displaced                   INTEGER,
    harassed                    BOOLEAN,
    property_damage             BOOLEAN,
    online_harassment           BOOLEAN,
    online_harassment_detail    TEXT,

    -- Perpetrator summary
    number_of_suspects          TEXT,
    hate_speech_made_by         TEXT,
    perpetrator_type            perpetrator_type_enum,

    -- Legal summary (detail in legal_actions)
    fir_status                  fir_status_enum,
    fir_number                  TEXT,
    fir_filed_date              DATE,
    fir_filed_against           TEXT,
    police_station               TEXT,
    ps_code                     TEXT,                  -- NCRB police station code
    police_role                 police_role_enum,
    investigating_agency        TEXT,
    other_investigating_agency  TEXT,
    investigating_officer       TEXT,
    cross_fir_filed             BOOLEAN,
    cross_fir_filed_date        DATE,
    cross_fir_filed_against     TEXT,
    cross_fir_charges           TEXT[],
    fir_charges                 TEXT[],
    case_current_status         TEXT,

    -- Context
    state_government_party      TEXT,
    hate_crime_included         TEXT[],
    type_of_discrimination      TEXT[],

    -- Deduplication
    canonical_incident_id       BIGINT REFERENCES incidents(id) ON DELETE SET NULL,
    duplicate_of                BOOLEAN DEFAULT FALSE,

    -- JotForm raw fields (kept for reference)
    victim_details_raw          TEXT,
    suspect_details_raw         TEXT,
    geolocation_raw             TEXT,

    -- Submission metadata
    submission_ip               TEXT,
    approval_status             approval_status_enum DEFAULT 'pending',
    verified_by                 TEXT,
    verified_at                 TIMESTAMPTZ,

    -- Provenance
    data_source                 data_source_enum DEFAULT 'jotform',
    ai_provider                 TEXT,
    ai_model                    TEXT,
    entered_by                  TEXT,
    reviewed_by                 TEXT,
    review_notes                TEXT,
    verification_status         verification_status_enum DEFAULT 'pending',
    reliability_level           reliability_level_enum DEFAULT '1',
    published                   BOOLEAN DEFAULT FALSE,
    sensitive                   BOOLEAN DEFAULT FALSE,
    retracted                   BOOLEAN DEFAULT FALSE,
    retraction_reason           TEXT,
    source_url                  TEXT,
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW(),
    deleted_at                  TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 4: incident_persons
-- Junction: incidents ↔ persons (with role)
-- =============================================================================
CREATE TABLE incident_persons (
    id                      BIGSERIAL PRIMARY KEY,
    incident_id             BIGINT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    person_id               BIGINT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    role                    incident_person_role_enum NOT NULL,
    direct_responsibility   BOOLEAN DEFAULT FALSE,
    notes                   TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (incident_id, person_id, role)
);


-- =============================================================================
-- TABLE 5: person_relationships
-- Links persons to each other — family, co-accused, targeting of relatives
-- =============================================================================
CREATE TABLE person_relationships (
    id                  BIGSERIAL PRIMARY KEY,
    person_id_a         BIGINT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    person_id_b         BIGINT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    relationship_type   relationship_type_enum NOT NULL,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (person_id_a, person_id_b, relationship_type)
);


-- =============================================================================
-- TABLE 6: legal_actions
-- One row per legal event — FIR, arrest, chargesheet, judgment etc.
-- =============================================================================
CREATE TABLE legal_actions (
    id                  BIGSERIAL PRIMARY KEY,
    incident_id         BIGINT REFERENCES incidents(id) ON DELETE CASCADE,

    -- What kind of legal action
    action_type         legal_action_type_enum NOT NULL,
    action_date         DATE,
    fir_number          TEXT,

    -- Who filed against whom
    filed_by_id         BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    filed_against_id    BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    filed_by_org_id     BIGINT REFERENCES organisations(id) ON DELETE SET NULL,

    -- Court details
    court_name          TEXT,
    court_level         court_level_enum,
    court_state         TEXT,
    judge_id            BIGINT REFERENCES persons(id) ON DELETE SET NULL,

    -- Charges
    charges             TEXT[],
    ipc_sections        TEXT[],                        -- e.g. ['153A','295A','302']
    outcome             TEXT,
    outcome_date        DATE,

    -- Status
    current_status      TEXT,
    next_date           DATE,

    -- Provenance
    data_source         data_source_enum DEFAULT 'manual',
    ai_provider         TEXT,
    ai_model            TEXT,
    entered_by          TEXT,
    reviewed_by         TEXT,
    review_notes        TEXT,
    verification_status verification_status_enum DEFAULT 'pending',
    reliability_level   reliability_level_enum DEFAULT '1',
    published           BOOLEAN DEFAULT FALSE,
    sensitive           BOOLEAN DEFAULT FALSE,
    retracted           BOOLEAN DEFAULT FALSE,
    retraction_reason   TEXT,
    source_url          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 7: case_timeline
-- Every hearing and order as a dated event under a legal_action
-- =============================================================================
CREATE TABLE case_timeline (
    id                  BIGSERIAL PRIMARY KEY,
    legal_action_id     BIGINT NOT NULL REFERENCES legal_actions(id) ON DELETE CASCADE,

    event_date          DATE NOT NULL,
    event_type          case_event_type_enum NOT NULL,
    court_name          TEXT,
    court_level         court_level_enum,
    judge_id            BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    description         TEXT,
    outcome             TEXT,
    next_date           DATE,
    document_url        TEXT,
    archived_url        TEXT,

    -- Provenance
    data_source         data_source_enum DEFAULT 'manual',
    ai_provider         TEXT,
    ai_model            TEXT,
    entered_by          TEXT,
    reviewed_by         TEXT,
    verification_status verification_status_enum DEFAULT 'pending',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 8: detentions
-- Pre-trial & political detention — UAPA, NSA, PSA
-- =============================================================================
CREATE TABLE detentions (
    id                          BIGSERIAL PRIMARY KEY,
    person_id                   BIGINT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    incident_id                 BIGINT REFERENCES incidents(id) ON DELETE SET NULL,
    legal_action_id             BIGINT REFERENCES legal_actions(id) ON DELETE SET NULL,

    -- Detention details
    detention_type              detention_type_enum NOT NULL,
    arrested_date               DATE,
    arrested_by_id              BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    arresting_org_id            BIGINT REFERENCES organisations(id) ON DELETE SET NULL,
    charges                     TEXT[],
    ipc_sections                TEXT[],
    jail_name                   TEXT,
    jail_state                  TEXT,

    -- Bail tracking
    bail_applied                BOOLEAN DEFAULT FALSE,
    bail_denied_count           INTEGER DEFAULT 0,
    bail_granted_date           DATE,

    -- Release
    release_date                DATE,
    days_detained               INTEGER GENERATED ALWAYS AS (
                                    CASE
                                        WHEN release_date IS NOT NULL AND arrested_date IS NOT NULL
                                        THEN (release_date - arrested_date)
                                        ELSE NULL
                                    END
                                ) STORED,
    released_by_higher_court    BOOLEAN DEFAULT FALSE,
    conviction_at_release       BOOLEAN DEFAULT FALSE,
    release_notes               TEXT,

    -- Pattern linking
    related_detention_id        BIGINT REFERENCES detentions(id) ON DELETE SET NULL,

    -- Provenance
    data_source                 data_source_enum DEFAULT 'manual',
    ai_provider                 TEXT,
    ai_model                    TEXT,
    entered_by                  TEXT,
    reviewed_by                 TEXT,
    review_notes                TEXT,
    verification_status         verification_status_enum DEFAULT 'pending',
    reliability_level           reliability_level_enum DEFAULT '1',
    published                   BOOLEAN DEFAULT FALSE,
    sensitive                   BOOLEAN DEFAULT FALSE,
    retracted                   BOOLEAN DEFAULT FALSE,
    retraction_reason           TEXT,
    source_url                  TEXT,
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW(),
    deleted_at                  TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 9: state_actions
-- Bulldozer orders, property seizure, false FIRs against victims
-- =============================================================================
CREATE TABLE state_actions (
    id                      BIGSERIAL PRIMARY KEY,
    incident_id             BIGINT REFERENCES incidents(id) ON DELETE CASCADE,

    action_type             state_action_type_enum NOT NULL,
    action_date             DATE,
    state                   TEXT,
    district                TEXT,
    state_code              CHAR(2),

    -- Who ordered it
    ordered_by_id           BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    ordering_org_id         BIGINT REFERENCES organisations(id) ON DELETE SET NULL,
    authority_claimed       BOOLEAN DEFAULT FALSE,

    -- Legitimacy
    court_order_exists      BOOLEAN DEFAULT FALSE,
    court_order_verified    BOOLEAN DEFAULT FALSE,
    court_order_number      TEXT,
    notice_given            BOOLEAN DEFAULT FALSE,
    notice_period_days      INTEGER,

    -- Scale
    affected_count          INTEGER,
    affected_persons        TEXT[],
    description             TEXT,

    -- Provenance
    data_source             data_source_enum DEFAULT 'manual',
    ai_provider             TEXT,
    ai_model                TEXT,
    entered_by              TEXT,
    reviewed_by             TEXT,
    review_notes            TEXT,
    verification_status     verification_status_enum DEFAULT 'pending',
    reliability_level       reliability_level_enum DEFAULT '1',
    published               BOOLEAN DEFAULT FALSE,
    sensitive               BOOLEAN DEFAULT FALSE,
    retracted               BOOLEAN DEFAULT FALSE,
    retraction_reason       TEXT,
    source_url              TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    deleted_at              TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 10: properties
-- Demolished homes, mosques, churches, dargahs, agricultural land
-- =============================================================================
CREATE TABLE properties (
    id                      BIGSERIAL PRIMARY KEY,
    state_action_id         BIGINT REFERENCES state_actions(id) ON DELETE CASCADE,
    incident_id             BIGINT REFERENCES incidents(id) ON DELETE SET NULL,

    property_type           property_type_enum NOT NULL,
    name                    TEXT,                      -- e.g. mosque name
    owner_name              TEXT,                      -- denormalised for search
    owner_id                BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    owner_religion          TEXT,
    owner_org_id            BIGINT REFERENCES organisations(id) ON DELETE SET NULL,

    -- Location
    address                 TEXT,
    district                TEXT,
    state                   TEXT,
    state_code              CHAR(2),
    latitude                NUMERIC(9,6),
    longitude               NUMERIC(9,6),

    -- Status
    demolished              BOOLEAN DEFAULT FALSE,
    demolition_date         DATE,
    seized                  BOOLEAN DEFAULT FALSE,
    seizure_date            DATE,
    court_order             BOOLEAN DEFAULT FALSE,
    court_order_verified    BOOLEAN DEFAULT FALSE,
    notice_given            BOOLEAN DEFAULT FALSE,
    notice_period_days      INTEGER,

    -- Impact
    families_displaced      INTEGER,
    estimated_value         NUMERIC(15,2),
    currency                TEXT DEFAULT 'INR',
    description             TEXT,

    -- Provenance
    data_source             data_source_enum DEFAULT 'manual',
    ai_provider             TEXT,
    ai_model                TEXT,
    entered_by              TEXT,
    reviewed_by             TEXT,
    review_notes            TEXT,
    verification_status     verification_status_enum DEFAULT 'pending',
    reliability_level       reliability_level_enum DEFAULT '1',
    published               BOOLEAN DEFAULT FALSE,
    sensitive               BOOLEAN DEFAULT FALSE,
    retracted               BOOLEAN DEFAULT FALSE,
    retraction_reason       TEXT,
    source_url              TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    deleted_at              TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 11: speeches
-- Hate speeches — one row per delivery/instance
-- =============================================================================
CREATE TABLE speeches (
    id                      BIGSERIAL PRIMARY KEY,
    person_id               BIGINT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    org_id                  BIGINT REFERENCES organisations(id) ON DELETE SET NULL,
    incident_id             BIGINT REFERENCES incidents(id) ON DELETE SET NULL,

    -- Speech details
    speech_date             DATE,
    platform                speech_platform_enum,
    venue                   TEXT,
    state                   TEXT,
    district                TEXT,
    content_summary         TEXT,
    direct_quote            TEXT,
    language                TEXT,
    targets                 bias_motivation_enum[],    -- who was targeted

    -- Media
    media_url               TEXT,
    archived_url            TEXT,
    transcript_url          TEXT,

    -- Impact
    estimated_reach         BIGINT,
    preceded_violence       BOOLEAN DEFAULT FALSE,
    days_before_incident    INTEGER,

    -- Legal response
    legal_action_taken      BOOLEAN DEFAULT FALSE,
    legal_action_id         BIGINT REFERENCES legal_actions(id) ON DELETE SET NULL,

    -- Provenance
    data_source             data_source_enum DEFAULT 'manual',
    ai_provider              TEXT,
    ai_model                TEXT,
    entered_by              TEXT,
    reviewed_by             TEXT,
    review_notes            TEXT,
    verification_status     verification_status_enum DEFAULT 'pending',
    reliability_level       reliability_level_enum DEFAULT '1',
    published               BOOLEAN DEFAULT FALSE,
    sensitive               BOOLEAN DEFAULT FALSE,
    retracted               BOOLEAN DEFAULT FALSE,
    retraction_reason       TEXT,
    source_url              TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    deleted_at              TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 12: media_coverage
-- News / TV coverage — incitement, false narrative, suppression
-- =============================================================================
CREATE TABLE media_coverage (
    id                      BIGSERIAL PRIMARY KEY,
    incident_id             BIGINT REFERENCES incidents(id) ON DELETE CASCADE,
    speech_id               BIGINT REFERENCES speeches(id) ON DELETE SET NULL,

    -- Who covered it
    outlet_id               BIGINT REFERENCES organisations(id) ON DELETE SET NULL,
    anchor_id               BIGINT REFERENCES persons(id) ON DELETE SET NULL,
    author_name             TEXT,

    -- Coverage details
    coverage_date           DATE,
    coverage_type           media_coverage_type_enum,
    headline                TEXT,
    content_summary         TEXT,
    language                TEXT,

    -- URLs
    url                     TEXT,
    archived_url            TEXT,

    -- Impact & fact-check
    preceded_violence       BOOLEAN DEFAULT FALSE,
    days_before_incident    INTEGER,
    estimated_reach         BIGINT,
    fact_checked            BOOLEAN DEFAULT FALSE,
    fact_check_url          TEXT,
    fact_check_outcome      TEXT,

    -- Provenance
    data_source             data_source_enum DEFAULT 'manual',
    ai_provider             TEXT,
    ai_model                TEXT,
    entered_by              TEXT,
    reviewed_by             TEXT,
    review_notes            TEXT,
    verification_status     verification_status_enum DEFAULT 'pending',
    reliability_level       reliability_level_enum DEFAULT '1',
    published               BOOLEAN DEFAULT FALSE,
    sensitive               BOOLEAN DEFAULT FALSE,
    retracted               BOOLEAN DEFAULT FALSE,
    retraction_reason       TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    deleted_at              TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 13: media_files
-- Images, videos, audio — archived to Hetzner (Step 2)
-- =============================================================================
CREATE TABLE media_files (
    id                  BIGSERIAL PRIMARY KEY,
    incident_id         BIGINT REFERENCES incidents(id) ON DELETE CASCADE,
    speech_id           BIGINT REFERENCES speeches(id) ON DELETE SET NULL,
    media_coverage_id   BIGINT REFERENCES media_coverage(id) ON DELETE SET NULL,

    file_type           TEXT,                          -- image/video/audio/document
    original_url        TEXT,
    hetzner_path        TEXT,
    archived            BOOLEAN DEFAULT FALSE,
    archived_at         TIMESTAMPTZ,
    platform            TEXT,                          -- instagram/youtube/whatsapp etc.
    file_size_bytes     BIGINT,
    mime_type           TEXT,
    description         TEXT,

    -- Provenance
    data_source         data_source_enum DEFAULT 'jotform',
    entered_by          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ
);


-- =============================================================================
-- TABLE 14: sources
-- News links, court docs, official records cited as evidence
-- =============================================================================
CREATE TABLE sources (
    id                  BIGSERIAL PRIMARY KEY,
    incident_id         BIGINT REFERENCES incidents(id) ON DELETE CASCADE,
    legal_action_id     BIGINT REFERENCES legal_actions(id) ON DELETE SET NULL,
    speech_id           BIGINT REFERENCES speeches(id) ON DELETE SET NULL,
    detention_id        BIGINT REFERENCES detentions(id) ON DELETE SET NULL,
    state_action_id     BIGINT REFERENCES state_actions(id) ON DELETE SET NULL,
    property_id         BIGINT REFERENCES properties(id) ON DELETE SET NULL,

    source_type         source_type_enum NOT NULL,
    title               TEXT,
    url                 TEXT,
    archived_url        TEXT,
    publication_date    DATE,
    author              TEXT,
    outlet              TEXT,
    language            TEXT,
    reliability         TEXT DEFAULT 'unverified',
    notes               TEXT,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- TABLE 15: ipc_sections
-- Reference table for IPC / BNS / UAPA section codes
-- =============================================================================
CREATE TABLE ipc_sections (
    id              BIGSERIAL PRIMARY KEY,
    section_code    TEXT NOT NULL UNIQUE,              -- e.g. '153A', 'UAPA-13'
    act_name        TEXT NOT NULL,                     -- IPC / BNS / UAPA / NIA
    description     TEXT,
    commonly_used_for TEXT,                            -- how this section is typically applied
    is_bailable     BOOLEAN,
    max_sentence    TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- TABLE 16: ai_staging
-- Buffer for all AI-extracted records pending human review
-- =============================================================================
CREATE TABLE ai_staging (
    id                  BIGSERIAL PRIMARY KEY,

    -- Source
    source_url          TEXT,
    source_type         source_type_enum,
    raw_text            TEXT,
    raw_json            JSONB,

    -- AI details
    ai_provider         TEXT NOT NULL,                 -- anthropic/openai/google
    ai_model            TEXT NOT NULL,
    ai_prompt_version   TEXT,
    confidence_score    NUMERIC(3,2),                  -- 0.00 to 1.00
    extraction_notes    TEXT,                          -- AI's own uncertainty flags

    -- Target
    target_table        TEXT,                          -- which table this maps to
    extracted_at        TIMESTAMPTZ DEFAULT NOW(),

    -- Review
    review_status       ai_review_status_enum DEFAULT 'pending',
    assigned_to         TEXT,
    reviewed_by         TEXT,
    reviewed_at         TIMESTAMPTZ,
    review_notes        TEXT,
    approved_record_id  BIGINT,                        -- ID of approved row in target table

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- TABLE 17: jotform_sync_log
-- Tracks each JotForm sync run
-- =============================================================================
CREATE TABLE jotform_sync_log (
    id                      BIGSERIAL PRIMARY KEY,
    synced_at               TIMESTAMPTZ DEFAULT NOW(),
    sync_type               TEXT DEFAULT 'incremental', -- incremental / full
    last_submission_id      TEXT,
    fetched                 INTEGER DEFAULT 0,
    new_records             INTEGER DEFAULT 0,
    updated_records         INTEGER DEFAULT 0,
    errors                  INTEGER DEFAULT 0,
    error_details           JSONB,
    duration_seconds        NUMERIC(8,2)
);


-- =============================================================================
-- INDEXES
-- =============================================================================

-- incidents — most queried table
CREATE INDEX idx_incidents_type        ON incidents(incident_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_incidents_state       ON incidents(state) WHERE deleted_at IS NULL;
CREATE INDEX idx_incidents_state_code  ON incidents(state_code) WHERE deleted_at IS NULL;
CREATE INDEX idx_incidents_date        ON incidents(incident_date) WHERE deleted_at IS NULL;
CREATE INDEX idx_incidents_severity    ON incidents(incident_severity) WHERE deleted_at IS NULL;
CREATE INDEX idx_incidents_published   ON incidents(published) WHERE deleted_at IS NULL;
CREATE INDEX idx_incidents_jotform_id  ON incidents(jotform_submission_id);
CREATE INDEX idx_incidents_canonical   ON incidents(canonical_incident_id);
CREATE INDEX idx_incidents_bias        ON incidents USING GIN(bias_motivation);
CREATE INDEX idx_incidents_geo         ON incidents(latitude, longitude) WHERE latitude IS NOT NULL;

-- persons
CREATE INDEX idx_persons_name          ON persons USING GIN(to_tsvector('simple', full_name));
CREATE INDEX idx_persons_type          ON persons(person_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_persons_org           ON persons(org_id);
CREATE INDEX idx_persons_state         ON persons(state);
CREATE INDEX idx_persons_aliases       ON persons USING GIN(aliases);

-- organisations
CREATE INDEX idx_orgs_type             ON organisations(org_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_orgs_name             ON organisations USING GIN(to_tsvector('simple', name));

-- legal_actions
CREATE INDEX idx_legal_incident        ON legal_actions(incident_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_legal_type            ON legal_actions(action_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_legal_judge           ON legal_actions(judge_id);
CREATE INDEX idx_legal_ipc             ON legal_actions USING GIN(ipc_sections);

-- case_timeline
CREATE INDEX idx_timeline_legal        ON case_timeline(legal_action_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_timeline_date         ON case_timeline(event_date) WHERE deleted_at IS NULL;

-- detentions
CREATE INDEX idx_detentions_person     ON detentions(person_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_detentions_type       ON detentions(detention_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_detentions_related    ON detentions(related_detention_id);

-- state_actions
CREATE INDEX idx_state_actions_incident ON state_actions(incident_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_state_actions_type     ON state_actions(action_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_state_actions_state    ON state_actions(state) WHERE deleted_at IS NULL;

-- properties
CREATE INDEX idx_properties_type       ON properties(property_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_properties_state      ON properties(state) WHERE deleted_at IS NULL;
CREATE INDEX idx_properties_geo        ON properties(latitude, longitude) WHERE latitude IS NOT NULL;

-- speeches
CREATE INDEX idx_speeches_person       ON speeches(person_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_speeches_date         ON speeches(speech_date) WHERE deleted_at IS NULL;
CREATE INDEX idx_speeches_targets      ON speeches USING GIN(targets);

-- media_coverage
CREATE INDEX idx_media_cov_incident    ON media_coverage(incident_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_media_cov_outlet      ON media_coverage(outlet_id);
CREATE INDEX idx_media_cov_anchor      ON media_coverage(anchor_id);

-- sources
CREATE INDEX idx_sources_incident      ON sources(incident_id);

-- ai_staging
CREATE INDEX idx_staging_status        ON ai_staging(review_status);
CREATE INDEX idx_staging_table         ON ai_staging(target_table);


-- =============================================================================
-- UPDATED_AT TRIGGER
-- =============================================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to all tables with updated_at
DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'incidents','persons','organisations','legal_actions',
        'case_timeline','detentions','state_actions','properties',
        'speeches','media_coverage','media_files','ai_staging'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_%I_updated_at
             BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION set_updated_at()',
            t, t
        );
    END LOOP;
END;
$$;


-- =============================================================================
-- SEED: IPC sections commonly used in communal / hate crime cases
-- =============================================================================
INSERT INTO ipc_sections (section_code, act_name, description, commonly_used_for, is_bailable, max_sentence) VALUES
('153A',    'IPC',  'Promoting enmity between groups',                  'Charged against journalists, activists, minorities for criticism of state', TRUE,  '3 years'),
('153B',    'IPC',  'Imputations prejudicial to national integration',  'Against minorities alleging anti-national activity',                        TRUE,  '3 years'),
('295A',    'IPC',  'Deliberate acts to outrage religious feelings',    'Against religious minorities for practising religion',                      TRUE,  '3 years'),
('302',     'IPC',  'Murder',                                           'Lynching cases',                                                            FALSE, 'Death / Life'),
('307',     'IPC',  'Attempt to murder',                                'Mob assault cases',                                                         FALSE, '10 years'),
('354',     'IPC',  'Assault on woman with intent to outrage modesty',  'Sexual violence in communal incidents',                                     FALSE, '5 years'),
('395',     'IPC',  'Dacoity',                                          'Mob looting during riots',                                                  FALSE, '10 years'),
('436',     'IPC',  'Mischief by fire to destroy house',                'Arson in communal violence',                                                FALSE, 'Life'),
('505',     'IPC',  'Statements conducing to public mischief',          'Against journalists and social media users',                                TRUE,  '3 years'),
('124A',    'IPC',  'Sedition',                                         'Against activists, journalists, minority leaders',                          FALSE, 'Life'),
('UAPA-13', 'UAPA', 'Punishment for terrorist activities',              'Against Muslim activists, students, journalists',                           FALSE, '5-life'),
('UAPA-18', 'UAPA', 'Punishment for conspiracy to commit terror act',   'Against alleged conspirators',                                              FALSE, 'Life'),
('NSA-3',   'NSA',  'Power to detain certain persons',                  'Administrative detention without trial, no bail',                           FALSE, 'Up to 12 months'),
('PSA-3',   'PSA',  'Power to detain (J&K Public Safety Act)',          'J&K detentions, Kashmiri journalists and activists',                        FALSE, 'Up to 2 years'),
('BNS-196', 'BNS',  'Promoting enmity (replaces IPC 153A)',             'New BNS equivalent — used post-2024',                                      TRUE,  '3 years');


-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
