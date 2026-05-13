/* @bruin
name: staging.stg_labour_market
type: duckdb.sql
depends:
    - raw.lfs_state
materialization:
    type: table
connection: duckdb-default

columns:
    - name: state
      description: "Malaysian state name"
      checks:
          - name: not_null
    - name: year
      description: "Reference year"
      checks:
          - name: not_null
          - name: positive
    - name: quarter
      description: "Quarter (1-4)"
      checks:
          - name: not_null
    - name: u_rate
      description: "Unemployment rate (%)"
      checks:
          - name: not_null
    - name: p_rate
      description: "Labour force participation rate (%)"
      checks:
          - name: not_null
@bruin */

SELECT
    TRIM(state)                             AS state,
    year,
    quarter,
    ROUND(CAST(u_rate AS DOUBLE), 2)        AS u_rate,
    ROUND(CAST(p_rate AS DOUBLE), 2)        AS p_rate,
    ROUND(CAST(lf AS DOUBLE), 1)            AS labour_force_thousands,
    ROUND(CAST(lf_employed AS DOUBLE), 1)   AS employed_thousands,
    ROUND(CAST(lf_unemployed AS DOUBLE), 1) AS unemployed_thousands,
    ROUND(CAST(lf_outside AS DOUBLE), 1)    AS outside_labour_force_thousands
FROM raw.lfs_state
ORDER BY state, year, quarter
