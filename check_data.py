"""
Instamart Data Pipeline - Data Sanity Check
=============================================
Run this after generate_data.py to confirm the 5 CSVs are structurally
sound before uploading to S3. Checks:
  1. Row counts
  2. Null counts per column
  3. Referential integrity (foreign keys actually exist)
  4. Order status distribution
  5. Store-level delivery time pattern (should show a few bad stores)
  6. Basic duplicate check

Run:  python check_data.py
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/bronze")

print("Loading CSVs...\n")
customers = pd.read_csv(DATA_DIR / "customers.csv")
products = pd.read_csv(DATA_DIR / "products.csv")
orders = pd.read_csv(DATA_DIR / "orders.csv")
order_items = pd.read_csv(DATA_DIR / "order_items.csv")
inventory = pd.read_csv(DATA_DIR / "inventory.csv")

tables = {
    "customers": customers,
    "products": products,
    "orders": orders,
    "order_items": order_items,
    "inventory": inventory,
}

# ------------------------------------------------------------
# 1. Row counts
# ------------------------------------------------------------
print("=" * 60)
print("1. ROW COUNTS")
print("=" * 60)
total = 0
for name, df in tables.items():
    print(f"  {name:15s}: {len(df):>12,} rows")
    total += len(df)
print(f"  {'TOTAL':15s}: {total:>12,} rows")

# ------------------------------------------------------------
# 2. Null counts
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("2. NULL COUNTS PER COLUMN")
print("=" * 60)
for name, df in tables.items():
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if len(nulls) == 0:
        print(f"  {name}: no nulls")
    else:
        print(f"  {name}:")
        for col, cnt in nulls.items():
            pct = cnt / len(df) * 100
            print(f"      {col:20s} {cnt:>8,} nulls ({pct:.2f}%)")

# ------------------------------------------------------------
# 3. Referential integrity
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("3. REFERENTIAL INTEGRITY (foreign keys)")
print("=" * 60)

valid_customer_ids = set(customers["customer_id"])
valid_product_ids = set(products["product_id"])
valid_order_ids = set(orders["order_id"])
valid_store_ids = set(orders["store_id"].dropna().unique())

orphan_orders = (~orders["customer_id"].isin(valid_customer_ids)).sum()
print(f"  orders.customer_id not in customers    : {orphan_orders:,}")

orphan_items_order = (~order_items["order_id"].isin(valid_order_ids)).sum()
print(f"  order_items.order_id not in orders     : {orphan_items_order:,}")

orphan_items_product = (~order_items["product_id"].isin(valid_product_ids)).sum()
print(f"  order_items.product_id not in products : {orphan_items_product:,}")

orphan_inventory_product = (~inventory["product_id"].isin(valid_product_ids)).sum()
print(f"  inventory.product_id not in products   : {orphan_inventory_product:,}")

# ------------------------------------------------------------
# 4. Order status distribution
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("4. ORDER STATUS DISTRIBUTION")
print("=" * 60)
status_counts = orders["order_status"].value_counts()
for status, cnt in status_counts.items():
    pct = cnt / len(orders) * 100
    print(f"  {status:12s}: {cnt:>10,} ({pct:.1f}%)")

# ------------------------------------------------------------
# 5. Store-level delivery time (worst performers)
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("5. WORST 5 STORES BY AVG DELIVERY TIME")
print("=" * 60)
store_delivery = (
    orders[orders["delivery_time_minutes"] > 0]  # exclude the bad negative rows
    .groupby("store_id")["delivery_time_minutes"]
    .mean()
    .sort_values(ascending=False)
    .head(5)
)
for store_id, avg_time in store_delivery.items():
    print(f"  store_id {int(store_id):>3}: {avg_time:.1f} min avg")

# ------------------------------------------------------------
# 6. Duplicate check
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("6. EXACT DUPLICATE ROWS")
print("=" * 60)
for name, df in tables.items():
    if name == "order_items":
        # order_item_id is a surrogate key and is always unique by design;
        # check duplicates on the actual business columns instead
        dupes = df.duplicated(subset=["order_id", "product_id", "quantity", "unit_price"]).sum()
    else:
        dupes = df.duplicated().sum()
    print(f"  {name:15s}: {dupes:,} duplicate rows")

# ------------------------------------------------------------
# 7. Negative / invalid values check
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("7. INVALID VALUES")
print("=" * 60)
neg_delivery = (orders["delivery_time_minutes"] < 0).sum()
print(f"  orders with negative delivery_time_minutes: {neg_delivery:,}")
neg_price = (order_items["unit_price"] < 0).sum()
print(f"  order_items with negative unit_price      : {neg_price:,}")
neg_stock = (inventory["stock_quantity"] < 0).sum()
print(f"  inventory with negative stock_quantity    : {neg_stock:,}")

print("\n" + "=" * 60)
print("DONE. This is exactly the kind of mess your Silver-layer")
print("cleaning job should catch and fix.")
print("=" * 60)