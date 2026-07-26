-- =============================================================================
-- geo_india — India geographic reference table
-- Based on India Post pincode database (165,627 post offices)
-- Deduplicated to 19,486 unique pincodes
-- =============================================================================

CREATE TABLE IF NOT EXISTS geo_india (
    id                  BIGSERIAL PRIMARY KEY,

    -- Postal
    pincode             TEXT NOT NULL UNIQUE,
    post_office         TEXT,
    division            TEXT,                  -- postal division

    -- Geography hierarchy
    village_area        TEXT,                  -- populated later from Census
    block_taluk         TEXT,                  -- populated later from Census
    district            TEXT NOT NULL,
    district_code       TEXT,                  -- NCRB district code (add later)
    state               TEXT NOT NULL,
    state_code          CHAR(2),
    union_territory     BOOLEAN DEFAULT FALSE,

    -- Coordinates (centroid of all post offices in this pincode)
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),

    -- Political (versioned separately in geo_political table)
    -- lok_sabha_constituency  TEXT,           -- add via geo_political
    -- vidhan_sabha_constituency TEXT,         -- add via geo_political

    -- Demographics (from Census — add later)
    minority_pct_muslim     NUMERIC(5,2),
    minority_pct_christian  NUMERIC(5,2),
    minority_pct_sikh       NUMERIC(5,2),
    total_population        INTEGER,

    -- Flags
    communally_sensitive    BOOLEAN DEFAULT FALSE,
    communally_sensitive_source TEXT,

    -- Meta
    office_count        INTEGER,               -- number of post offices in pincode
    data_source         TEXT DEFAULT 'india_post',
    last_updated        DATE DEFAULT CURRENT_DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast lookup
CREATE INDEX IF NOT EXISTS idx_geo_india_pincode   ON geo_india(pincode);
CREATE INDEX IF NOT EXISTS idx_geo_india_district  ON geo_india(district);
CREATE INDEX IF NOT EXISTS idx_geo_india_state     ON geo_india(state);
CREATE INDEX IF NOT EXISTS idx_geo_india_state_code ON geo_india(state_code);
CREATE INDEX IF NOT EXISTS idx_geo_india_geo       ON geo_india(latitude, longitude);

-- =============================================================================
-- geo_political — time-versioned political constituency data
-- New election = new rows. Old rows preserved for historical queries.
-- =============================================================================

CREATE TABLE IF NOT EXISTS geo_political (
    id                          BIGSERIAL PRIMARY KEY,

    -- Geography reference
    pincode                     TEXT REFERENCES geo_india(pincode) ON DELETE CASCADE,
    district                    TEXT,
    state                       TEXT,

    -- Lok Sabha
    lok_sabha_constituency      TEXT,
    lok_sabha_constituency_no   INTEGER,
    ls_mp_name                  TEXT,
    ls_mp_party                 TEXT,
    ls_election_year            INTEGER,

    -- Vidhan Sabha
    vidhan_sabha_constituency   TEXT,
    vidhan_sabha_no             INTEGER,
    vs_mla_name                 TEXT,
    vs_mla_party                TEXT,
    vs_election_year            INTEGER,

    -- Ruling party at state level during this period
    state_ruling_party          TEXT,
    state_cm_name               TEXT,

    -- Time bounds — critical for historical queries
    valid_from                  DATE NOT NULL,
    valid_to                    DATE,          -- NULL means currently valid

    -- Source
    data_source                 TEXT,          -- 'ECI', 'TCPD', 'manual'
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_geo_pol_pincode  ON geo_political(pincode);
CREATE INDEX IF NOT EXISTS idx_geo_pol_state    ON geo_political(state);
CREATE INDEX IF NOT EXISTS idx_geo_pol_ls       ON geo_political(lok_sabha_constituency);
CREATE INDEX IF NOT EXISTS idx_geo_pol_vs       ON geo_political(vidhan_sabha_constituency);
CREATE INDEX IF NOT EXISTS idx_geo_pol_valid    ON geo_political(valid_from, valid_to);
