"""
Instamart Data Pipeline - Silver Layer (Data Cleaning)
=========================================================
Reads raw CSVs from data/bronze/, applies data-quality cleaning,
and writes clean tables as Parquet to data/silver/.

Cleaning rules applied (matches the intentional messiness injected
by generate_data.py):

  customers:
    - null `area`      -> filled with "Unknown" (row kept, other
                           columns are still useful)
    - duplicate rows    -> dropped

  orders:
    - null `store_id`   -> row dropped (an order with no store is
                           not usable for store-level analysis)
    - negative
      `delivery_time_minutes` -> converted to positive (abs()),
                           treated as a sign-entry error

  order_items, products, inventory:
    - no known issues at generation time, but basic type/negative
      checks are still applied defensively

Run:  python silver_layer.py
Output: data/silver/customers.parquet, orders.parquet,
        order_items.parquet, products.parquet, inventory.parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------------------
# 1. Start Spark
# ----------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("instamart-silver-layer")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # hide noisy INFO logs

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"

print("=" * 60)
print("SILVER LAYER - DATA CLEANING")
print("=" * 60)


def load_csv(name):
    """Read a bronze CSV with header + inferred schema."""
    path = f"{BRONZE_DIR}/{name}.csv"
    return spark.read.csv(path, header=True, inferSchema=True)


def report(name, before, after):
    print(f"  {name:15s}: {before:>10,} rows -> {after:>10,} rows "
          f"({before - after:,} removed)")


# ----------------------------------------------------------------
# 2. CUSTOMERS
# ----------------------------------------------------------------
print("\nCleaning customers...")
customers = load_csv("customers")
before = customers.count()

customers_clean = (
    customers
    .fillna({"area": "Unknown"})   # null area -> "Unknown"
    .dropDuplicates()               # remove exact duplicate rows
)

after = customers_clean.count()
report("customers", before, after)
customers_clean.write.mode("overwrite").parquet(f"{SILVER_DIR}/customers.parquet")


# ----------------------------------------------------------------
# 3. ORDERS
# ----------------------------------------------------------------
print("\nCleaning orders...")
orders = load_csv("orders")
before = orders.count()

orders_clean = (
    orders
    .dropna(subset=["store_id"])                          # drop rows with no store
    .withColumn(
        "delivery_time_minutes",
        F.abs(F.col("delivery_time_minutes"))              # fix negative values
    )
)

after = orders_clean.count()
report("orders", before, after)
orders_clean.write.mode("overwrite").parquet(f"{SILVER_DIR}/orders.parquet")


# ----------------------------------------------------------------
# 4. ORDER_ITEMS
# ----------------------------------------------------------------
print("\nCleaning order_items...")
order_items = load_csv("order_items")
before = order_items.count()

order_items_clean = (
    order_items
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
)

after = order_items_clean.count()
report("order_items", before, after)
order_items_clean.write.mode("overwrite").parquet(f"{SILVER_DIR}/order_items.parquet")


# ----------------------------------------------------------------
# 5. PRODUCTS
# ----------------------------------------------------------------
print("\nCleaning products...")
products = load_csv("products")
before = products.count()

products_clean = (
    products
    .dropDuplicates(["product_id"])
    .filter(F.col("price") >= 0)
)

after = products_clean.count()
report("products", before, after)
products_clean.write.mode("overwrite").parquet(f"{SILVER_DIR}/products.parquet")


# ----------------------------------------------------------------
# 6. INVENTORY
# ----------------------------------------------------------------
print("\nCleaning inventory...")
inventory = load_csv("inventory")
before = inventory.count()

inventory_clean = (
    inventory
    .withColumn(
        "stock_quantity",
        F.when(F.col("stock_quantity") < 0, 0).otherwise(F.col("stock_quantity"))
    )  # negative stock doesn't make sense -> floor at 0
)

after = inventory_clean.count()
report("inventory", before, after)
inventory_clean.write.mode("overwrite").parquet(f"{SILVER_DIR}/inventory.parquet")


# ----------------------------------------------------------------
# DONE
# ----------------------------------------------------------------
print("\n" + "=" * 60)
print("SILVER LAYER COMPLETE")
print(f"Clean Parquet tables written to: {SILVER_DIR}/")
print("=" * 60)

spark.stop()