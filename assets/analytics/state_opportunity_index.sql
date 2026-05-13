/* @bruin
name: analytics.state_opportunity_index
type: duckdb.sql
depends:
    - staging.stg_income_inequality
    - staging.stg_labour_market
materialization:
    type: table
connection: duckdb-default

columns:
    - name: state
      checks:
          - name: not_null
    - name: income_median
      checks:
          - name: not_null
          - name: positive
    - name: gini
      checks:
          - name: not_null
    - name: avg_unemployment
      checks:
          - name: not_null
@bruin */

WITH latest_income AS (
    SELECT state, year, gini, poverty, income_mean, income_median,
           expenditure_mean, income_skew, income_skew_pct
    FROM staging.stg_income_inequality
    WHERE year = (SELECT MAX(year) FROM staging.stg_income_inequality)
),
latest_labour AS (
    SELECT state,
           ROUND(AVG(u_rate), 2) AS avg_unemployment,
           ROUND(AVG(p_rate), 2) AS avg_participation,
           ROUND(AVG(labour_force_thousands), 1) AS avg_labour_force
    FROM staging.stg_labour_market
    WHERE year = (SELECT MAX(year) FROM staging.stg_labour_market)
    GROUP BY state
)
SELECT
    i.state,
    i.year AS survey_year,
    i.income_median,
    i.income_mean,
    i.gini,
    i.poverty,
    i.income_skew,
    i.income_skew_pct,
    l.avg_unemployment,
    l.avg_participation,
    l.avg_labour_force,
    RANK() OVER (ORDER BY i.income_median DESC) AS income_rank,
    RANK() OVER (ORDER BY i.gini ASC) AS equality_rank,
    RANK() OVER (ORDER BY l.avg_unemployment ASC) AS employment_rank
FROM latest_income i
LEFT JOIN latest_labour l ON i.state = l.state
WHERE l.state IS NOT NULL
ORDER BY income_rank
