"""
Instamart Data Pipeline - Gold Layer Data Quality Tests
==========================================================
Sanity-checks the 6 Gold-layer Parquet tables after they're built.
Run manually (python test_gold_layer.py) or as an Airflow task.

Checks per table:
  1. File exists
  2. Row count > 0
  3. No nulls in key columns
  4. No negative values in numeric columns that should never be negative

Exits with code 1 (failure) if any check fails, so it can be used
as a pass/fail gate in Airflow.
"""

import sys
from pathlib import Path
import pandas as pd

GOLD_DIR = Path("data/gold")

# table_name -> (key columns that must never be null, numeric columns that must never be negative)
CHECKS = {
    "daily_orders": (
        ["order_date"],
        ["total_orders", "cancelled_orders"],
    ),
    "product_performance": (
        ["product_id", "product_name", "category"],
        ["units_sold", "revenue"],
    ),
    "category_performance": (
        ["category"],
        ["units_sold", "revenue"],
    ),
    "inventory_health": (
        ["product_id", "product_name"],
        ["total_stock", "total_demand"],
    ),
    "delivery_performance": (
        ["store_id"],
        ["total_orders", "avg_delivery_minutes", "late_deliveries"],
    ),
    "location_insights": (
        ["city", "area"],
        ["total_orders", "total_revenue"],
    ),
}

print("=" * 60)
print("GOLD LAYER - DATA QUALITY TESTS")
print("=" * 60)

failures = []

for table_name, (key_cols, numeric_cols) in CHECKS.items():
    print(f"\nChecking {table_name}...")
    file_path = GOLD_DIR / f"{table_name}.parquet"

    # 1. File exists
    if not file_path.exists():
        print(f"  [FAIL] File not found: {file_path}")
        failures.append(f"{table_name}: file not found")
        continue

    df = pd.read_parquet(file_path)

    # 2. Row count > 0
    if len(df) == 0:
        print(f"  [FAIL] Table is empty")
        failures.append(f"{table_name}: empty table")
        continue
    print(f"  [OK] {len(df):,} rows")

    # 3. No nulls in key columns
    for col in key_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(f"  [FAIL] {col}: {null_count} null values")
            failures.append(f"{table_name}.{col}: {null_count} nulls")
        else:
            print(f"  [OK] {col}: no nulls")

    # 4. No negative values in numeric columns
    for col in numeric_cols:
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            print(f"  [FAIL] {col}: {neg_count} negative values")
            failures.append(f"{table_name}.{col}: {neg_count} negative values")
        else:
            print(f"  [OK] {col}: no negative values")

print("\n" + "=" * 60)
if failures:
    print(f"RESULT: FAILED ({len(failures)} issue(s) found)")
    print("=" * 60)
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print("RESULT: ALL CHECKS PASSED")
    print("=" * 60)
    sys.exit(0)