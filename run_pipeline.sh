#!/bin/bash
set -e
echo "Running Malaysia Talent Drain Pipeline..."

echo "[1/11] Ingesting HIES State..."
bruin run assets/raw/raw_hies_state.py

echo "[2/11] Ingesting HIES District..."
bruin run assets/raw/raw_hies_district.py

echo "[3/11] Ingesting Labour Force (State)..."
bruin run assets/raw/raw_lfs_state.py

echo "[4/11] Ingesting Labour Force (Monthly)..."
bruin run assets/raw/raw_lfs_monthly.py

echo "[5/11] Ingesting CPI..."
bruin run assets/raw/raw_cpi.py

echo "[6/11] Ingesting Exchange Rate..."
bruin run assets/raw/raw_exchange_rate.py

echo "[7/11] Ingesting Citizenship Data..."
bruin run assets/raw/raw_citizenship.py

echo "[8/11] Staging: Income Inequality..."
bruin run assets/staging/stg_income_inequality.sql

echo "[9/11] Staging: Labour Market..."
bruin run assets/staging/stg_labour_market.sql

echo "[10/11] Staging: Cost of Living..."
bruin run assets/staging/stg_cost_of_living.sql

echo "[11/11] Staging: Brain Drain..."
bruin run assets/staging/stg_brain_drain.sql

echo "[12/14] Analytics: State Opportunity Index..."
bruin run assets/analytics/state_opportunity_index.sql

echo "[13/14] Analytics: Brain Drain Trend..."
bruin run assets/analytics/brain_drain_trend.sql

echo "[14/14] Analytics: Income vs Cost..."
bruin run assets/analytics/income_vs_cost.sql

echo "Pipeline complete."
