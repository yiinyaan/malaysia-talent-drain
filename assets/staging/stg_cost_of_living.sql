/* @bruin
name: staging.stg_cost_of_living
type: duckdb.sql
depends:
    - raw.cpi
materialization:
    type: table
connection: duckdb-default

columns:
    - name: year
      checks:
          - name: not_null
    - name: month
      checks:
          - name: not_null
    - name: division
      description: "CPI category"
      checks:
          - name: not_null
    - name: cpi_index
      description: "Consumer Price Index value"
      checks:
          - name: not_null
          - name: positive
@bruin */

SELECT
    year,
    month,
    TRIM(division)                          AS division,
    ROUND(CAST(index AS DOUBLE), 2)         AS cpi_index
FROM raw.cpi
WHERE index IS NOT NULL
ORDER BY division, year, month
