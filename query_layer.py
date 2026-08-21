"""
Instamart Data Pipeline - Query Layer (DuckDB)
==================================================
Runs SQL queries directly against the Gold-layer Parquet files to
answer every business question from the original project plan.

DuckDB needs no server, no setup, and no data loading step -- it
queries Parquet files on disk directly, the same way AWS Athena
queries Parquet on S3. This is the local equivalent of Athena.

Run:  python query_layer.py
"""

import duckdb

GOLD_DIR = "data/gold"
con = duckdb.connect()  # in-memory DuckDB session


def run(title, sql):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    result = con.execute(sql).fetchdf()
    print(result.to_string(index=False))


print("=" * 70)
print("INSTAMART BUSINESS QUESTIONS - ANSWERED VIA DUCKDB")
print("=" * 70)


# ==================================================================
# 📦 ORDERS
# ==================================================================

run(
    "Total orders, cancelled orders, and overall average order value",
    f"""
    SELECT
        SUM(total_orders)      AS total_orders,
        SUM(cancelled_orders)  AS total_cancelled_orders,
        ROUND(SUM(cancelled_orders) * 100.0 / SUM(total_orders), 2) AS cancel_rate_pct,
        ROUND(AVG(avg_order_value), 2) AS overall_avg_order_value
    FROM '{GOLD_DIR}/daily_orders.parquet'
    """
)

run(
    "Daily orders trend (first 10 days)",
    f"""
    SELECT order_date, total_orders, cancelled_orders, avg_order_value
    FROM '{GOLD_DIR}/daily_orders.parquet'
    ORDER BY order_date
    LIMIT 10
    """
)


# ==================================================================
# 🛍️ PRODUCTS
# ==================================================================

run(
    "Top 10 best-selling products by revenue",
    f"""
    SELECT product_id, product_name, category, units_sold, revenue
    FROM '{GOLD_DIR}/product_performance.parquet'
    ORDER BY revenue DESC
    LIMIT 10
    """
)

run(
    "Revenue by category (all categories, ranked)",
    f"""
    SELECT category, units_sold, revenue,
           ROUND(revenue * 100.0 / SUM(revenue) OVER (), 1) AS pct_of_total_revenue
    FROM '{GOLD_DIR}/category_performance.parquet'
    ORDER BY revenue DESC
    """
)


# ==================================================================
# 📊 INVENTORY
# ==================================================================

run(
    "Stock-out rate (products currently at zero stock)",
    f"""
    SELECT
        COUNT(*) AS total_products,
        SUM(CASE WHEN is_stock_out THEN 1 ELSE 0 END) AS stocked_out_products,
        ROUND(SUM(CASE WHEN is_stock_out THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS stock_out_rate_pct
    FROM '{GOLD_DIR}/inventory_health.parquet'
    """
)

run(
    "Top 10 low-stock products with highest demand (most urgent restocks)",
    f"""
    SELECT product_id, product_name, category, total_stock, total_demand
    FROM '{GOLD_DIR}/inventory_health.parquet'
    WHERE is_low_stock = TRUE
    ORDER BY total_demand DESC
    LIMIT 10
    """
)


# ==================================================================
# 🚚 DELIVERY
# ==================================================================

run(
    "Delivery performance by store (worst performers first)",
    f"""
    SELECT store_id, total_orders, avg_delivery_minutes,
           late_deliveries, late_delivery_pct
    FROM '{GOLD_DIR}/delivery_performance.parquet'
    ORDER BY avg_delivery_minutes DESC
    """
)

run(
    "Overall late delivery rate across all stores",
    f"""
    SELECT
        SUM(total_orders) AS total_orders,
        SUM(late_deliveries) AS total_late_deliveries,
        ROUND(SUM(late_deliveries) * 100.0 / SUM(total_orders), 2) AS overall_late_pct
    FROM '{GOLD_DIR}/delivery_performance.parquet'
    """
)


# ==================================================================
# 📍 LOCATION
# ==================================================================

run(
    "Top 10 areas by revenue",
    f"""
    SELECT city, area, total_orders, total_revenue
    FROM '{GOLD_DIR}/location_insights.parquet'
    ORDER BY total_revenue DESC
    LIMIT 10
    """
)

run(
    "Revenue by city (rolled up across all areas)",
    f"""
    SELECT city,
           SUM(total_orders) AS total_orders,
           ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM '{GOLD_DIR}/location_insights.parquet'
    GROUP BY city
    ORDER BY total_revenue DESC
    """
)

print("\n" + "=" * 70)
print("ALL BUSINESS QUESTIONS ANSWERED")
print("=" * 70)
