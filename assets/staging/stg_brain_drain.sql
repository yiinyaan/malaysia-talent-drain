/* @bruin
name: staging.stg_brain_drain
type: duckdb.sql
depends:
    - raw.citizenship_renunciation
    - raw.exchange_rate
materialization:
    type: table
connection: duckdb-default

columns:
    - name: year
      checks:
          - name: not_null
          - name: positive
    - name: renunciations
      description: "Number of Malaysians who renounced citizenship for Singapore"
      checks:
          - name: not_null
          - name: positive
@bruin */

SELECT
    c.year,
    CAST(c.renunciations_to_singapore AS INTEGER) AS renunciations,
    c.source,
    e.myr_to_sgd,
    ROUND(1.0 / e.myr_to_sgd, 4)                 AS sgd_to_myr
FROM raw.citizenship_renunciation c
LEFT JOIN (
    SELECT year, AVG(myr_to_sgd) AS myr_to_sgd
    FROM raw.exchange_rate
    GROUP BY year
) e ON c.year = e.year
ORDER BY c.year
