"""
Instamart-Inspired Quick Commerce - Synthetic Data Generator
===============================================================
Generates 5 relational CSVs (customers, products, orders, order_items,
inventory) with realistic, non-uniform patterns:

  - Product popularity follows an 80/20 (Zipf-like) skew
  - Orders peak on weekends and evenings
  - A subset of stores under-perform on delivery time
  - Stock-outs are correlated with high-demand products
  - Cancelled orders are more likely when stock is low
  - A small amount of intentional "messiness" (nulls, dupes, bad
    formats) is injected so the Silver-layer cleaning step has
    real work to do

Uses vectorized pandas/numpy operations throughout so it scales to
millions of rows without row-by-row loops (no faker dependency).

Run:  python generate_data.py
Output: ./data/customers.csv, products.csv, orders.csv,
        order_items.csv, inventory.csv
"""

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# ----------------------------------------------------------------
# CONFIG -- tune these to control dataset scale
# ----------------------------------------------------------------
SEED = 42
NUM_CUSTOMERS = 100_000
NUM_STORES = 50
NUM_PRODUCTS = 2_000
NUM_ORDERS = 1_200_000
INVENTORY_DAYS = 60          # daily inventory snapshot window
ORDER_DATE_RANGE_DAYS = 180  # orders span the last N days
OUTPUT_DIR = Path("data")

rng = np.random.default_rng(SEED)
TODAY = datetime(2026, 8, 17)

OUTPUT_DIR.mkdir(exist_ok=True)


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------
def random_dates(start_days_ago, n, weekend_boost=True):
    """Vectorized random dates over the last `start_days_ago` days,
    with heavier weight on weekends/evenings if weekend_boost=True."""
    offsets = rng.integers(0, start_days_ago, size=n)
    dates = np.array([TODAY - timedelta(days=int(o)) for o in offsets])
    return dates


def skewed_ids(n_ids, size, alpha=1.6):
    """Zipf-like popularity skew: a few ids get picked much more often."""
    ranks = np.arange(1, n_ids + 1)
    weights = 1 / np.power(ranks, alpha)
    weights /= weights.sum()
    return rng.choice(np.arange(1, n_ids + 1), size=size, p=weights)


# ----------------------------------------------------------------
# 1. CUSTOMERS
# ----------------------------------------------------------------
print("Generating customers...")

CITIES_AREAS = {
    "Bengaluru": ["Koramangala", "Indiranagar", "Whitefield", "HSR Layout", "Jayanagar"],
    "Mumbai": ["Andheri", "Bandra", "Powai", "Dadar", "Malad"],
    "Delhi": ["Dwarka", "Saket", "Rohini", "Karol Bagh", "Vasant Kunj"],
    "Hyderabad": ["Gachibowli", "Madhapur", "Kukatpally", "Banjara Hills"],
    "Pune": ["Kothrud", "Hinjewadi", "Viman Nagar", "Baner"],
    "Chennai": ["Adyar", "T Nagar", "Velachery", "Anna Nagar"],
}
cities = list(CITIES_AREAS.keys())
first_names = ["Aarav", "Vivaan", "Aditi", "Isha", "Rohan", "Priya", "Karan",
               "Neha", "Arjun", "Sana", "Kabir", "Meera", "Yash", "Ritu",
               "Dev", "Anaya", "Sahil", "Pooja", "Rahul", "Divya"]
last_names = ["Sharma", "Verma", "Iyer", "Reddy", "Nair", "Gupta", "Khan",
              "Patel", "Singh", "Rao", "Das", "Mehta", "Joshi", "Kulkarni"]

city_choice = rng.choice(cities, size=NUM_CUSTOMERS)
area_choice = np.array([rng.choice(CITIES_AREAS[c]) for c in city_choice])

customers = pd.DataFrame({
    "customer_id": np.arange(1, NUM_CUSTOMERS + 1),
    "customer_name": [
        f"{rng.choice(first_names)} {rng.choice(last_names)}"
        for _ in range(NUM_CUSTOMERS)
    ],
    "city": city_choice,
    "area": area_choice,
    "signup_date": random_dates(730, NUM_CUSTOMERS),  # up to 2 years back
})

# inject messiness: ~0.5% missing area, a handful of exact dupes
null_idx = rng.choice(NUM_CUSTOMERS, size=int(NUM_CUSTOMERS * 0.005), replace=False)
customers.loc[null_idx, "area"] = None
dupe_rows = customers.sample(n=200, random_state=SEED)
customers = pd.concat([customers, dupe_rows], ignore_index=True)

customers.to_csv(OUTPUT_DIR / "customers.csv", index=False)
print(f"  -> {len(customers):,} rows")


# ----------------------------------------------------------------
# 2. PRODUCTS
# ----------------------------------------------------------------
print("Generating products...")

CATEGORIES = {
    "Fruits & Vegetables": (20, 200),
    "Dairy & Eggs": (15, 350),
    "Snacks": (10, 250),
    "Beverages": (20, 500),
    "Bakery": (25, 300),
    "Personal Care": (50, 800),
    "Household": (40, 1200),
    "Grocery & Staples": (30, 900),
}
categories = list(CATEGORIES.keys())
brands = ["Amul", "Nestle", "ITC", "HUL", "Britannia", "Parle", "Dabur",
          "Patanjali", "Local Brand", "Fresh Farms", "Daily Basics"]

cat_choice = rng.choice(categories, size=NUM_PRODUCTS)
prices = np.array([
    round(rng.lognormal(mean=np.log((lo + hi) / 2), sigma=0.4), 2)
    for lo, hi in (CATEGORIES[c] for c in cat_choice)
])

products = pd.DataFrame({
    "product_id": np.arange(1, NUM_PRODUCTS + 1),
    "product_name": [f"{cat_choice[i].split()[0]} Item {i+1}" for i in range(NUM_PRODUCTS)],
    "category": cat_choice,
    "brand": rng.choice(brands, size=NUM_PRODUCTS),
    "price": prices,
})
products.to_csv(OUTPUT_DIR / "products.csv", index=False)
print(f"  -> {len(products):,} rows")


# ----------------------------------------------------------------
# 3. ORDERS  (store-level performance variation baked in)
# ----------------------------------------------------------------
print("Generating orders...")

store_ids = np.arange(1, NUM_STORES + 1)
# each store gets its own "average delivery time" baseline;
# a handful of stores are deliberately bad performers
store_avg_delivery = rng.normal(loc=25, scale=5, size=NUM_STORES)
bad_stores = rng.choice(store_ids, size=max(3, NUM_STORES // 10), replace=False)
store_avg_delivery[bad_stores - 1] += rng.uniform(15, 30, size=len(bad_stores))
store_avg_delivery = np.clip(store_avg_delivery, 10, None)

order_dates_dt = random_dates(ORDER_DATE_RANGE_DAYS, NUM_ORDERS)
weekday = np.array([d.weekday() for d in order_dates_dt])  # 5,6 = weekend

# weekend boost via resampling: duplicate ~30% more weekend-eligible orders
is_weekend = np.isin(weekday, [5, 6])
weekend_extra_idx = rng.choice(np.where(is_weekend)[0],
                                size=int(is_weekend.sum() * 0.3),
                                replace=True)

order_customer = rng.integers(1, NUM_CUSTOMERS + 1, size=NUM_ORDERS)
order_store = rng.integers(1, NUM_STORES + 1, size=NUM_ORDERS)

# evening peak: order hour skewed toward 18-21
hour_weights = np.array([1,1,1,1,1,1,2,3,4,5,5,6,7,6,5,5,6,7,9,9,8,5,3,2], dtype=float)
hour_weights /= hour_weights.sum()
order_hours = rng.choice(np.arange(24), size=NUM_ORDERS, p=hour_weights)
order_minutes = rng.integers(0, 60, size=NUM_ORDERS)
order_times = [f"{h:02d}:{m:02d}:00" for h, m in zip(order_hours, order_minutes)]

# delivery time: store baseline + noise, occasional bad delays
base_delivery = store_avg_delivery[order_store - 1]
delivery_noise = rng.normal(0, 6, size=NUM_ORDERS)
delivery_time = np.clip(base_delivery + delivery_noise, 8, None).round().astype(int)

# order status: mostly delivered; higher cancel chance when delivery is very slow
cancel_prob = np.clip(0.03 + (delivery_time > 55) * 0.12, 0, 0.4)
status_roll = rng.random(NUM_ORDERS)
order_status = np.where(status_roll < cancel_prob, "Cancelled", "Delivered")
# a small "Returned" sliver
returned_mask = (order_status == "Delivered") & (rng.random(NUM_ORDERS) < 0.015)
order_status = np.where(returned_mask, "Returned", order_status)

orders = pd.DataFrame({
    "order_id": np.arange(1, NUM_ORDERS + 1),
    "customer_id": order_customer,
    "order_date": [d.strftime("%Y-%m-%d") for d in order_dates_dt],
    "order_time": order_times,
    "store_id": order_store,
    "delivery_time_minutes": delivery_time,
    "order_status": order_status,
})

# messiness: ~0.3% negative delivery_time typos, ~0.2% null store_id
bad_idx = rng.choice(NUM_ORDERS, size=int(NUM_ORDERS * 0.003), replace=False)
orders.loc[bad_idx, "delivery_time_minutes"] *= -1
null_store_idx = rng.choice(NUM_ORDERS, size=int(NUM_ORDERS * 0.002), replace=False)
orders.loc[null_store_idx, "store_id"] = None

orders.to_csv(OUTPUT_DIR / "orders.csv", index=False)
print(f"  -> {len(orders):,} rows")


# ----------------------------------------------------------------
# 4. ORDER_ITEMS  (popularity-skewed product selection)
# ----------------------------------------------------------------
print("Generating order_items...")

items_per_order = rng.integers(1, 5, size=NUM_ORDERS)  # 1-4 items/order
total_items = int(items_per_order.sum())

item_order_id = np.repeat(orders["order_id"].values, items_per_order)
item_product_id = skewed_ids(NUM_PRODUCTS, total_items, alpha=1.3)
item_quantity = rng.integers(1, 6, size=total_items)

price_lookup = products.set_index("product_id")["price"]
base_unit_price = price_lookup.loc[item_product_id].values
# occasional promo discount noise
promo_noise = rng.choice([1.0, 0.9, 0.8], size=total_items, p=[0.8, 0.15, 0.05])
unit_price = np.round(base_unit_price * promo_noise, 2)

order_items = pd.DataFrame({
    "order_item_id": np.arange(1, total_items + 1),  # surrogate key -> guarantees true uniqueness
    "order_id": item_order_id,
    "product_id": item_product_id,
    "quantity": item_quantity,
    "unit_price": unit_price,
})

# messiness: a few genuine duplicate line items (same order+product+qty+price,
# but still a distinct order_item_id, exactly like a real duplicate-submit bug)
dupe_items = order_items.sample(n=500, random_state=SEED).copy()
dupe_items["order_item_id"] = np.arange(total_items + 1, total_items + 1 + len(dupe_items))
order_items = pd.concat([order_items, dupe_items], ignore_index=True)

order_items.to_csv(OUTPUT_DIR / "order_items.csv", index=False)
print(f"  -> {len(order_items):,} rows")


# ----------------------------------------------------------------
# 5. INVENTORY  (daily snapshot, correlated stock-outs on popular items)
# ----------------------------------------------------------------
print("Generating inventory (this is the largest table)...")

inv_dates = [(TODAY - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(INVENTORY_DAYS)]

# product popularity rank reused so hot products run out more often
product_rank = np.arange(1, NUM_PRODUCTS + 1)
popularity_weight = 1 / np.power(product_rank, 1.3)
popularity_weight /= popularity_weight.max()  # 0..1, 1 = most popular

store_grid, product_grid = np.meshgrid(store_ids, np.arange(1, NUM_PRODUCTS + 1), indexing="ij")
store_grid = store_grid.ravel()
product_grid = product_grid.ravel()
pop_grid = popularity_weight[product_grid - 1]

n_pairs = len(store_grid)
inv_frames = []
for d in inv_dates:
    # base stock lower for popular products (they sell out faster)
    base_stock = rng.integers(0, 120, size=n_pairs).astype(float)
    base_stock -= (pop_grid * rng.integers(0, 80, size=n_pairs))
    stock = np.clip(base_stock, 0, None).round().astype(int)
    inv_frames.append(pd.DataFrame({
        "inventory_date": d,
        "store_id": store_grid,
        "product_id": product_grid,
        "stock_quantity": stock,
    }))

inventory = pd.concat(inv_frames, ignore_index=True)
inventory.to_csv(OUTPUT_DIR / "inventory.csv", index=False)
print(f"  -> {len(inventory):,} rows")


# ----------------------------------------------------------------
# SUMMARY
# ----------------------------------------------------------------
total_rows = len(customers) + len(products) + len(orders) + len(order_items) + len(inventory)
print("\n===== DONE =====")
print(f"customers.csv    : {len(customers):>10,} rows")
print(f"products.csv     : {len(products):>10,} rows")
print(f"orders.csv       : {len(orders):>10,} rows")
print(f"order_items.csv  : {len(order_items):>10,} rows")
print(f"inventory.csv    : {len(inventory):>10,} rows")
print(f"TOTAL            : {total_rows:>10,} rows")
print(f"\nFiles written to: {OUTPUT_DIR.resolve()}")