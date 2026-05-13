# Malaysia Talent Drain Monitor

A data engineering pipeline that quantifies Malaysia's brain drain crisis and maps economic inequality across all 16 states — built with Bruin and DuckDB.

## The Problem

Malaysia is on the verge of high-income nation status. But beneath the headline GDP numbers lies a troubling paradox: **82,318 Malaysians permanently renounced their citizenship for Singapore between 2015–2024**, with 2024 alone seeing a record 16,930 — a 95.8% year-over-year surge.

Meanwhile, the income gap between states tells a story of two Malaysias:

| State | Median Income (RM) | Poverty Rate |
|-------|-------------------|-------------|
| W.P. Kuala Lumpur | 10,234 | 1.4% |
| Kelantan | 3,614 | 13.2% |
| Sabah | 4,577 | 19.7% |

**A young graduate in Kelantan earns 2.8x less than one in KL.** When Singapore offers 3x the salary across the border, the math becomes simple — and the nation loses.

This pipeline asks: **What is the real cost of Malaysia's talent drain, and which states are most at risk?**

## Architecture
OpenDOSM API          Frankfurter API       Manual CSV Seed
(HIES, LFS, CPI)     (MYR/SGD rate)        (KDN citizenship data)
|                    |                      |
v                    v                      v
raw.hies_state      raw.exchange_rate    raw.citizenship_renunciation
raw.hies_district
raw.lfs_state
raw.lfs_monthly
raw.cpi
|                    |                      |
+--------------------+----------------------+
|
v
Staging Layer (data cleaning, type casting, null handling)
stg_income_inequality | stg_labour_market | stg_cost_of_living | stg_brain_drain
|
v
Analytics Layer (business logic, rankings, trends)
state_opportunity_index | brain_drain_trend | income_vs_cost

**Stack:** Bruin CLI · DuckDB · Python · OpenDOSM API · Frankfurter API

**Pipeline:** 7 raw assets · 4 staging assets · 3 analytics assets · 41 quality checks — all passing

## Key Findings

### Brain Drain: Accelerating Crisis

| Year | Renunciations | YoY Change | Severity |
|------|--------------|------------|----------|
| 2019 | 9,773 | +11.3% | High |
| 2020 | 4,474 | -54.2% | Low (borders closed) |
| 2021 | 3,335 | -25.5% | Low (borders closed) |
| 2022 | 5,768 | +73.0% | Moderate |
| 2023 | 8,648 | +49.9% | High |
| 2024 | 16,930 | +95.8% | Critical |

**Cumulative loss: 82,318 citizens to Singapore alone (2015–2024)**. The post-COVID surge suggests pent-up demand — people who wanted to leave during lockdowns finally did.

### State Inequality: Two Malaysias

| Rank | State | Median Income | Gini | Unemployment | Poverty |
|------|-------|--------------|------|-------------|---------|
| 1 | W.P. Kuala Lumpur | RM 10,234 | 0.380 | 3.0% | 1.4% |
| 2 | W.P. Putrajaya | RM 10,056 | 0.368 | 1.5% | 0.1% |
| 3 | Selangor | RM 9,983 | 0.361 | 2.0% | 1.5% |
| ... | | | | | |
| 14 | Perak | RM 4,494 | 0.368 | 3.4% | 7.5% |
| 15 | Kedah | RM 4,402 | 0.359 | 2.5% | 9.0% |
| 16 | Kelantan | RM 3,614 | 0.385 | 4.4% | 13.2% |

The top 3 states (KL, Putrajaya, Selangor) — all in the Klang Valley — have median incomes more than **double** the bottom 3. Sabah stands out with the highest poverty rate at 19.7%.

### The Push Factor: Why People Leave

When a Malaysian earns RM 3,614/month in Kelantan but could earn SGD equivalent of RM 10,000+ in Singapore, the economic incentive is overwhelming. The MYR/SGD exchange rate compounds this — every ringgit earned in Singapore stretches further when sent home.

## Data Sources

| Source | Datasets | Method |
|--------|----------|--------|
| [OpenDOSM API](https://open.dosm.gov.my) | HIES (income, Gini, poverty), LFS (unemployment, participation), CPI | Python API calls |
| [Frankfurter API](https://frankfurter.app) | MYR/SGD exchange rate | Python API calls |
| KDN Parliamentary Replies | Citizenship renunciation to Singapore | Manual CSV seed |

### A Note on Data Gaps

Some critical datasets for a complete talent drain analysis — migration flow data, wage statistics by skill level, overseas diaspora tracking — are not yet available via API. Malaysia's statistical infrastructure is still maturing. This pipeline works with what is publicly accessible today, and is designed to incorporate new data sources as they become available.

## Pipeline Structure
malaysia-talent-drain/
├── pipeline.yml
├── requirements.txt
├── run_pipeline.sh
├── seeds/
│   └── citizenship_renunciation.csv
├── assets/
│   ├── raw/
│   │   ├── raw_hies_state.py
│   │   ├── raw_hies_district.py
│   │   ├── raw_lfs_state.py
│   │   ├── raw_lfs_monthly.py
│   │   ├── raw_cpi.py
│   │   ├── raw_exchange_rate.py
│   │   └── raw_citizenship.py
│   ├── staging/
│   │   ├── stg_income_inequality.sql
│   │   ├── stg_labour_market.sql
│   │   ├── stg_cost_of_living.sql
│   │   └── stg_brain_drain.sql
│   └── analytics/
│       ├── state_opportunity_index.sql
│       ├── brain_drain_trend.sql
│       └── income_vs_cost.sql
├── data/
│   └── malaysia.duckdb (generated)
└── README.md

## Getting Started

### Prerequisites
- [Bruin CLI](https://getbruin.com)
- Python 3.11+
- WSL2 (Windows) or Linux/macOS

### Run the Pipeline

```bash
git clone https://github.com/YOUR_USERNAME/malaysia-talent-drain.git
cd malaysia-talent-drain

# Run full pipeline (sequential to avoid DuckDB lock conflicts)
chmod +x run_pipeline.sh
./run_pipeline.sh
```

Expected output: 14 assets executed, 41 quality checks passed.

### Query Results

```bash
# State opportunity ranking
bruin query --connection duckdb-default --query \
  "SELECT state, income_median, gini, poverty, income_rank
   FROM analytics.state_opportunity_index ORDER BY income_rank"

# Brain drain trend
bruin query --connection duckdb-default --query \
  "SELECT year, renunciations, cumulative_loss, drain_severity
   FROM analytics.brain_drain_trend ORDER BY year"
```

## Quality Checks

**41 quality checks — all passing**, including:
- `not_null` on all key columns across all staging and analytics assets
- `positive` validation on income, population, and economic indicators
- Referential integrity via `depends` declarations ensuring correct execution order

## Why This Matters

Malaysia loses more than citizens when people leave for Singapore. It loses tax revenue, innovation capacity, mentors for the next generation, and — most importantly — the belief that staying and building locally can lead to a good life.

This pipeline doesn't solve the problem. But it makes the cost visible. And what gets measured gets managed.

---

*Built for the Data Engineering Zoomcamp 2026 — Bruin Project Competition*
