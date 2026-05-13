/* @bruin
name: staging.stg_income_inequality
type: duckdb.sql
depends:
    - raw.hies_state
materialization:
    type: table
connection: duckdb-default

columns:
    - name: state
      description: "Malaysian state name"
      checks:
          - name: not_null
    - name: year
      description: "Survey year"
      checks:
          - name: not_null
          - name: positive
    - name: gini
      description: "Gini coefficient (0-1)"
      checks:
          - name: not_null
    - name: income_mean
      description: "Mean household income (RM)"
      checks:
          - name: not_null
          - name: positive
    - name: income_median
      description: "Median household income (RM)"
      checks:
          - name: not_null
          - name: positive
    - name: poverty
      description: "Poverty rate (%)"
      checks:
          - name: not_null
@bruin */

SELECT
    TRIM(state)                                         AS state,
    year,
    ROUND(CAST(gini AS DOUBLE), 4)                      AS gini,
    ROUND(CAST(poverty AS DOUBLE), 2)                   AS poverty,
    CAST(income_mean AS INTEGER)                         AS income_mean,
    CAST(income_median AS INTEGER)                       AS income_median,
    CAST(expenditure_mean AS INTEGER)                    AS expenditure_mean,
    ROUND(CAST(income_mean AS DOUBLE) - CAST(income_median AS DOUBLE), 0) AS income_skew,
    ROUND((CAST(income_mean AS DOUBLE) - CAST(income_median AS DOUBLE)) / CAST(income_mean AS DOUBLE) * 100, 2) AS income_skew_pct
FROM raw.hies_state
ORDER BY state, year
