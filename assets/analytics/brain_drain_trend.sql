/* @bruin
name: analytics.brain_drain_trend
type: duckdb.sql
depends:
    - staging.stg_brain_drain
    - staging.stg_cost_of_living
materialization:
    type: table
connection: duckdb-default

columns:
    - name: year
      checks:
          - name: not_null
          - name: positive
    - name: renunciations
      checks:
          - name: not_null
          - name: positive
    - name: cumulative_loss
      checks:
          - name: not_null
          - name: positive
@bruin */

WITH cpi_annual AS (
    SELECT year,
           ROUND(AVG(cpi_index), 2) AS avg_cpi
    FROM staging.stg_cost_of_living
    WHERE division = 'overall'
    GROUP BY year
)
SELECT
    b.year,
    b.renunciations,
    SUM(b.renunciations) OVER (ORDER BY b.year) AS cumulative_loss,
    b.renunciations - LAG(b.renunciations) OVER (ORDER BY b.year) AS yoy_change,
    ROUND(
        (b.renunciations - LAG(b.renunciations) OVER (ORDER BY b.year)) * 100.0
        / LAG(b.renunciations) OVER (ORDER BY b.year), 2
    ) AS yoy_change_pct,
    b.myr_to_sgd,
    b.sgd_to_myr,
    c.avg_cpi,
    CASE
        WHEN b.renunciations >= 10000 THEN 'critical'
        WHEN b.renunciations >= 7000 THEN 'high'
        WHEN b.renunciations >= 5000 THEN 'moderate'
        ELSE 'low'
    END AS drain_severity
FROM staging.stg_brain_drain b
LEFT JOIN cpi_annual c ON b.year = c.year
ORDER BY b.year
