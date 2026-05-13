/* @bruin
name: analytics.income_vs_cost
type: duckdb.sql
depends:
    - staging.stg_income_inequality
    - staging.stg_cost_of_living
materialization:
    type: table
connection: duckdb-default

columns:
    - name: year
      checks:
          - name: not_null
          - name: positive
    - name: national_median_income
      checks:
          - name: not_null
          - name: positive
    - name: avg_cpi
      checks:
          - name: not_null
          - name: positive
@bruin */

WITH national_income AS (
    SELECT year,
           ROUND(AVG(income_median), 0) AS national_median_income,
           ROUND(AVG(income_mean), 0) AS national_mean_income,
           ROUND(AVG(gini), 4) AS national_gini,
           ROUND(AVG(poverty), 2) AS national_poverty,
           MAX(income_median) AS highest_state_median,
           MIN(income_median) AS lowest_state_median,
           MAX(income_median) - MIN(income_median) AS state_income_gap
    FROM staging.stg_income_inequality
    GROUP BY year
),
cpi_annual AS (
    SELECT year,
           ROUND(AVG(cpi_index), 2) AS avg_cpi
    FROM staging.stg_cost_of_living
    WHERE division = 'overall'
    GROUP BY year
)
SELECT
    n.year,
    n.national_median_income,
    n.national_mean_income,
    n.national_gini,
    n.national_poverty,
    n.highest_state_median,
    n.lowest_state_median,
    n.state_income_gap,
    c.avg_cpi,
    LAG(n.national_median_income) OVER (ORDER BY n.year) AS prev_income,
    ROUND(
        (n.national_median_income - LAG(n.national_median_income) OVER (ORDER BY n.year)) * 100.0
        / LAG(n.national_median_income) OVER (ORDER BY n.year), 2
    ) AS income_growth_pct,
    LAG(c.avg_cpi) OVER (ORDER BY n.year) AS prev_cpi,
    ROUND(
        (c.avg_cpi - LAG(c.avg_cpi) OVER (ORDER BY n.year)) * 100.0
        / LAG(c.avg_cpi) OVER (ORDER BY n.year), 2
    ) AS cpi_growth_pct
FROM national_income n
LEFT JOIN cpi_annual c ON n.year = c.year
ORDER BY n.year
