-- =============================================================================
-- INTERCONNECTION QUEUE DATABASE SCHEMA v2
-- =============================================================================
-- A normalized, well-structured schema for comprehensive queue analysis
-- =============================================================================

-- =============================================================================
-- DIMENSION TABLES (Slowly Changing Dimensions)
-- =============================================================================

-- Developers (canonical list with deduplication)
CREATE TABLE IF NOT EXISTS dim_developers (
    developer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    display_name TEXT,
    parent_company TEXT,
    parent_company_id INTEGER REFERENCES dim_developers(developer_id),
    developer_type TEXT,  -- 'IPP', 'Utility', 'Investor', 'Unknown'
    headquarters_state TEXT,
    website TEXT,
    founded_year INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Developer name aliases (for matching)
CREATE TABLE IF NOT EXISTS dim_developer_aliases (
    alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
    developer_id INTEGER NOT NULL REFERENCES dim_developers(developer_id),
    alias_name TEXT NOT NULL,
    source TEXT,  -- 'lbl', 'eia', 'ferc', 'manual'
    UNIQUE(alias_name)
);

-- Regions (ISO/RTO)
CREATE TABLE IF NOT EXISTS dim_regions (
    region_id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_code TEXT NOT NULL UNIQUE,  -- 'MISO', 'PJM', etc.
    region_name TEXT,
    region_type TEXT,  -- 'ISO', 'RTO', 'Utility', 'Other'
    timezone TEXT,
    website TEXT
);

-- Locations (geographic hierarchy)
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT NOT NULL,
    county TEXT,
    latitude REAL,
    longitude REAL,
    region_id INTEGER REFERENCES dim_regions(region_id),
    transmission_zone TEXT,
    lmp_node TEXT,
    UNIQUE(state, county)
);

-- Technologies (generation types)
CREATE TABLE IF NOT EXISTS dim_technologies (
    technology_id INTEGER PRIMARY KEY AUTOINCREMENT,
    technology_code TEXT NOT NULL UNIQUE,  -- 'solar', 'wind', 'gas', etc.
    technology_name TEXT,
    technology_category TEXT,  -- 'Renewable', 'Thermal', 'Storage', 'Hybrid'
    is_renewable BOOLEAN,
    is_dispatchable BOOLEAN,
    typical_capacity_factor REAL,
    typical_capex_per_kw REAL
);

-- Substations / Points of Interconnection
CREATE TABLE IF NOT EXISTS dim_substations (
    substation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    substation_name TEXT NOT NULL,
    region_id INTEGER REFERENCES dim_regions(region_id),
    location_id INTEGER REFERENCES dim_locations(location_id),
    voltage_kv REAL,
    owner TEXT,
    latitude REAL,
    longitude REAL,
    UNIQUE(substation_name, region_id)
);

-- Project Status (standardized statuses)
CREATE TABLE IF NOT EXISTS dim_statuses (
    status_id INTEGER PRIMARY KEY AUTOINCREMENT,
    status_code TEXT NOT NULL UNIQUE,
    status_name TEXT,
    status_category TEXT,  -- 'Active', 'Completed', 'Withdrawn', 'Suspended'
    sort_order INTEGER
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- Main projects fact table
CREATE TABLE IF NOT EXISTS fact_projects (
    project_id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Natural keys
    queue_id TEXT NOT NULL,
    region_id INTEGER NOT NULL REFERENCES dim_regions(region_id),

    -- Descriptive
    project_name TEXT,

    -- Foreign keys to dimensions
    developer_id INTEGER REFERENCES dim_developers(developer_id),
    location_id INTEGER REFERENCES dim_locations(location_id),
    technology_id INTEGER REFERENCES dim_technologies(technology_id),
    status_id INTEGER REFERENCES dim_statuses(status_id),
    poi_substation_id INTEGER REFERENCES dim_substations(substation_id),

    -- Measures
    capacity_mw REAL,
    capacity_mw_summer REAL,
    capacity_mw_winter REAL,

    -- Dates (standardized)
    queue_date DATE,
    cod_proposed DATE,
    cod_actual DATE,
    withdrawal_date DATE,

    -- Contract status
    has_ppa BOOLEAN,
    ppa_seller TEXT,
    ppa_price_mwh REAL,
    ppa_term_years INTEGER,

    -- Cost estimates
    interconnection_cost_m REAL,
    network_upgrade_cost_m REAL,

    -- Metadata
    data_source TEXT,  -- 'nyiso', 'ercot', 'miso', 'lbl', etc.
    source_record_id TEXT,
    first_seen_date DATE,
    last_updated_date DATE,
    row_hash TEXT,

    UNIQUE(queue_id, region_id)
);

-- Project history (track changes over time)
CREATE TABLE IF NOT EXISTS fact_project_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES fact_projects(project_id),
    snapshot_date DATE NOT NULL,

    -- Snapshot of key fields
    status_id INTEGER,
    capacity_mw REAL,
    cod_proposed DATE,
    developer_id INTEGER,

    -- Change tracking
    changed_fields TEXT,  -- JSON array of changed field names

    UNIQUE(project_id, snapshot_date)
);

-- =============================================================================
-- MARKET DATA TABLES
-- =============================================================================

-- LMP prices (hourly/daily)
CREATE TABLE IF NOT EXISTS fact_lmp_prices (
    lmp_id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_id INTEGER NOT NULL REFERENCES dim_regions(region_id),
    location_id INTEGER REFERENCES dim_locations(location_id),
    node_name TEXT,
    price_date DATE NOT NULL,
    hour INTEGER,  -- NULL for daily averages
    lmp_energy REAL,
    lmp_congestion REAL,
    lmp_losses REAL,
    lmp_total REAL
);

-- Capacity market results
CREATE TABLE IF NOT EXISTS fact_capacity_prices (
    capacity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_id INTEGER NOT NULL REFERENCES dim_regions(region_id),
    zone_name TEXT,
    delivery_year INTEGER NOT NULL,
    auction_type TEXT,  -- 'BRA', 'Incremental', etc.
    clearing_price_kw_month REAL,
    cleared_mw REAL,
    auction_date DATE
);

-- ELCC values by technology
CREATE TABLE IF NOT EXISTS fact_elcc (
    elcc_id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_id INTEGER NOT NULL REFERENCES dim_regions(region_id),
    technology_id INTEGER REFERENCES dim_technologies(technology_id),
    year INTEGER NOT NULL,
    elcc_percent REAL,
    methodology TEXT
);

-- =============================================================================
-- DEVELOPER INTELLIGENCE TABLES
-- =============================================================================

-- Developer portfolio summary (materialized/updated periodically)
CREATE TABLE IF NOT EXISTS mart_developer_portfolios (
    developer_id INTEGER PRIMARY KEY REFERENCES dim_developers(developer_id),

    -- Project counts
    total_projects INTEGER,
    active_projects INTEGER,
    operational_projects INTEGER,
    withdrawn_projects INTEGER,

    -- Capacity
    total_capacity_mw REAL,
    active_capacity_mw REAL,
    operational_capacity_mw REAL,

    -- Geographic
    states_count INTEGER,
    regions_count INTEGER,
    primary_region TEXT,
    primary_state TEXT,

    -- Technology mix
    solar_pct REAL,
    wind_pct REAL,
    storage_pct REAL,
    gas_pct REAL,

    -- Performance metrics
    avg_time_to_cod_days INTEGER,
    withdrawal_rate REAL,
    success_rate REAL,

    -- Financials (if available)
    avg_project_size_mw REAL,
    ppa_coverage_rate REAL,

    last_updated DATE
);

-- =============================================================================
-- ANALYTICS VIEWS
-- =============================================================================

-- Regional pipeline summary
CREATE VIEW IF NOT EXISTS v_regional_pipeline AS
SELECT
    r.region_code,
    t.technology_code,
    s.status_category,
    COUNT(*) as project_count,
    SUM(p.capacity_mw) as total_mw,
    AVG(p.capacity_mw) as avg_project_mw,
    MIN(p.queue_date) as earliest_queue,
    MAX(p.queue_date) as latest_queue
FROM fact_projects p
JOIN dim_regions r ON p.region_id = r.region_id
LEFT JOIN dim_technologies t ON p.technology_id = t.technology_id
LEFT JOIN dim_statuses s ON p.status_id = s.status_id
GROUP BY r.region_code, t.technology_code, s.status_category;

-- Project risk indicators
CREATE VIEW IF NOT EXISTS v_project_risk AS
SELECT
    p.project_id,
    p.queue_id,
    r.region_code,
    p.project_name,
    p.capacity_mw,
    d.canonical_name as developer,

    -- Time in queue
    JULIANDAY('now') - JULIANDAY(p.queue_date) as days_in_queue,

    -- Risk factors
    CASE WHEN p.has_ppa THEN 0 ELSE 1 END as no_ppa_risk,
    CASE WHEN d.developer_id IS NULL THEN 1 ELSE 0 END as unknown_developer_risk,
    CASE WHEN p.capacity_mw > 500 THEN 1 ELSE 0 END as large_project_risk,
    CASE WHEN JULIANDAY('now') - JULIANDAY(p.queue_date) > 1095 THEN 1 ELSE 0 END as long_queue_risk

FROM fact_projects p
JOIN dim_regions r ON p.region_id = r.region_id
LEFT JOIN dim_developers d ON p.developer_id = d.developer_id
WHERE p.status_id IN (SELECT status_id FROM dim_statuses WHERE status_category = 'Active');

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_projects_region ON fact_projects(region_id);
CREATE INDEX IF NOT EXISTS idx_projects_developer ON fact_projects(developer_id);
CREATE INDEX IF NOT EXISTS idx_projects_technology ON fact_projects(technology_id);
CREATE INDEX IF NOT EXISTS idx_projects_status ON fact_projects(status_id);
CREATE INDEX IF NOT EXISTS idx_projects_queue_date ON fact_projects(queue_date);
CREATE INDEX IF NOT EXISTS idx_projects_location ON fact_projects(location_id);

CREATE INDEX IF NOT EXISTS idx_history_project ON fact_project_history(project_id);
CREATE INDEX IF NOT EXISTS idx_history_date ON fact_project_history(snapshot_date);

CREATE INDEX IF NOT EXISTS idx_lmp_region_date ON fact_lmp_prices(region_id, price_date);
CREATE INDEX IF NOT EXISTS idx_developer_aliases ON dim_developer_aliases(alias_name);
