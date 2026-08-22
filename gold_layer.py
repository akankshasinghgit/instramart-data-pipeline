"""
Instamart Data Pipeline - Gold Layer (Business Metrics)
==========================================================
Reads clean tables from data/silver/, joins and aggregates them into
business-ready metric tables, and writes each as Parquet to data/gold/.

Produces 5 Gold tables, one per business category from the original
project plan:

  1. gold_daily_orders          -> Orders metrics (daily)
  2. gold_product_performance   -> Product/category metrics
  3. gold_inventory_health      -> Inventory/stock-out metrics
  4. gold_delivery_performance  -> Delivery metrics (by store)
  5. gold_location_insights     -> Location/area metrics

Run:  python gold_layer.py
Output: data/gold/*.parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------------------
# 1. Start Spark
# ----------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("instamart-gold-layer")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

print("=" * 60)
print("GOLD LAYER - BUSINESS METRICS")
print("=" * 60)


def load(name):
    return spark.read.parquet(f"{SILVER_DIR}/{name}.parquet")


def save(df, name):
    # Spark writes Parquet as a folder of "part files" (normal Spark behavior
    # for distributed writes), but Power BI / Excel / most BI tools expect a
    # single .parquet FILE. Since Gold tables are small aggregates (not the
    # full 10M-row datasets), we convert to pandas and write one clean file.
    pdf = df.toPandas()
    pdf.to_parquet(f"{GOLD_DIR}/{name}.parquet", index=False)
    print(f"  -> saved {name}.parquet ({len(pdf):,} rows)")


# ----------------------------------------------------------------
# Load all Silver tables once
# ----------------------------------------------------------------
customers = load("customers")
orders = load("orders")
order_items = load("order_items")
products = load("products")
inventory = load("inventory")

# Reusable: order_id -> total order value (quantity * unit_price, summed)
order_value = (
    order_items
    .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    .groupBy("order_id")
    .agg(F.sum("line_total").alias("order_value"))
)


# ==================================================================
# 1. ORDERS METRICS -> gold_daily_orders
# ==================================================================
print("\n[1/5] Building gold_daily_orders...")

orders_with_value = orders.join(order_value, on="order_id", how="left")

gold_daily_orders = (
    orders_with_value
    .groupBy("order_date")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum(F.when(F.col("order_status") == "Cancelled", 1).otherwise(0))
            .alias("cancelled_orders"),
        F.round(F.avg("order_value"), 2).alias("avg_order_value"),
    )
    .orderBy("order_date")
)
save(gold_daily_orders, "daily_orders")


# ==================================================================
# 2. PRODUCT METRICS -> gold_product_performance
# ==================================================================
print("\n[2/5] Building gold_product_performance...")

item_revenue = order_items.withColumn(
    "line_total", F.col("quantity") * F.col("unit_price")
)

product_sales = (
    item_revenue
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("revenue"),
    )
)

gold_product_performance = (
    product_sales
    .join(products, on="product_id", how="left")
    .select(
        "product_id", "product_name", "category", "brand",
        "units_sold", "revenue"
    )
    .orderBy(F.desc("revenue"))
)
save(gold_product_performance, "product_performance")

# Category-level rollup (top categories, revenue by category)
gold_category_performance = (
    gold_product_performance
    .groupBy("category")
    .agg(
        F.sum("units_sold").alias("units_sold"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    )
    .orderBy(F.desc("revenue"))
)
save(gold_category_performance, "category_performance")


# ==================================================================
# 3. INVENTORY METRICS -> gold_inventory_health
# ==================================================================
print("\n[3/5] Building gold_inventory_health...")

# Latest inventory snapshot per store/product (most recent date)
latest_date = inventory.agg(F.max("inventory_date")).collect()[0][0]

latest_inventory = inventory.filter(F.col("inventory_date") == latest_date)

# Demand per product (units sold), to compare against current stock
product_demand = (
    order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_demand"))
)

gold_inventory_health = (
    latest_inventory
    .groupBy("product_id")
    .agg(F.sum("stock_quantity").alias("total_stock"))
    .join(product_demand, on="product_id", how="left")
    .join(products.select("product_id", "product_name", "category"), on="product_id", how="left")
    .fillna({"total_demand": 0})
    .withColumn(
        "is_low_stock",
        F.col("total_stock") < 10  # threshold: fewer than 10 units left
    )
    .withColumn(
        "is_stock_out",
        F.col("total_stock") == 0
    )
    .select(
        "product_id", "product_name", "category",
        "total_stock", "total_demand", "is_low_stock", "is_stock_out"
    )
    .orderBy("total_stock")
)
save(gold_inventory_health, "inventory_health")


# ==================================================================
# 4. DELIVERY METRICS -> gold_delivery_performance
# ==================================================================
print("\n[4/5] Building gold_delivery_performance...")

gold_delivery_performance = (
    orders
    .groupBy("store_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("delivery_time_minutes"), 1).alias("avg_delivery_minutes"),
        F.sum(F.when(F.col("delivery_time_minutes") > 45, 1).otherwise(0))
            .alias("late_deliveries"),  # threshold: over 45 min = late
    )
    .withColumn(
        "late_delivery_pct",
        F.round(F.col("late_deliveries") / F.col("total_orders") * 100, 1)
    )
    .orderBy(F.desc("avg_delivery_minutes"))
)
save(gold_delivery_performance, "delivery_performance")


# ==================================================================
# 5. LOCATION METRICS -> gold_location_insights
# ==================================================================
print("\n[5/5] Building gold_location_insights...")

orders_with_customer = (
    orders_with_value
    .join(customers.select("customer_id", "city", "area"), on="customer_id", how="left")
)

gold_location_insights = (
    orders_with_customer
    .groupBy("city", "area")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("order_value"), 2).alias("total_revenue"),
    )
    .orderBy(F.desc("total_revenue"))
)
save(gold_location_insights, "location_insights")


# ----------------------------------------------------------------
# DONE
# ----------------------------------------------------------------
print("\n" + "=" * 60)
print("GOLD LAYER COMPLETE")
print(f"6 business-ready tables written to: {GOLD_DIR}/")
print("=" * 60)

spark.stop()