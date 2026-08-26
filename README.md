# Instamart-Inspired Quick Commerce Data Pipeline

A local, end-to-end data engineering project simulating a quick-commerce (Instamart-style) analytics pipeline — from raw synthetic data to business-ready dashboards.

> **Status:** ✅ v1.0 — All 8 weeks complete (Data Foundation → Bronze → Silver → Gold → Query Layer → Dashboard → Orchestration → Testing & Polish)

---

## 📌 Business Problem

A quick-commerce company operating multiple dark stores has data about customers, orders, products, inventory, and delivery — but scattered across different systems. This pipeline automates collecting, cleaning, transforming, and analyzing this data to answer:

> **Which products are in demand, where are stock-outs happening, how are deliveries performing, and which areas/products generate the most revenue?**

---

## 🏗️ Architecture

This project follows a **medallion architecture** (Bronze → Silver → Gold), built entirely with **local, free/open-source tools** — no cloud costs involved.

```
Raw CSVs (Bronze)
      │  PySpark — cleaning, dedup, null handling
      ▼
Clean Parquet Tables (Silver)
      │  PySpark — business logic & aggregations
      ▼
Aggregated Metrics (Gold, Parquet)
      │  Data quality tests (row counts, nulls, negative values)
      ▼
Validated Gold Tables
      │  DuckDB — SQL analytics
      ▼
Business Insights
      │  Power BI Desktop
      ▼
Interactive Dashboard
      │  Apache Airflow (Docker) — orchestrates the whole chain
      ▼
Automated, self-validating pipeline run
```

**Why local instead of AWS?** This project intentionally mirrors AWS's architecture (S3 → Glue → Athena → Power BI → MWAA) using free local equivalents (local folders → PySpark → DuckDB → Power BI Desktop → Docker Airflow), avoiding cloud billing risk while learning the same distributed-data-processing concepts. See [Design Decisions](#-design-decisions) below for the full reasoning.

| AWS Service (reference) | Local Equivalent (this project) |
|---|---|
| S3 | Local folders (`data/bronze/`, `data/silver/`, `data/gold/`) |
| Glue / PySpark ETL | PySpark (local) |
| Athena | DuckDB |
| Power BI (cloud-connected) | Power BI Desktop |
| MWAA (Managed Airflow) | Apache Airflow via Docker (local) |

---

## 🗂️ Data Model

5 relational tables, generated as realistic synthetic data (see [Why Synthetic Data?](#-why-synthetic-data)):

```
customers
    │ customer_id
    ▼
  orders
    │ order_id
    ▼
order_items
    │ product_id
    ▼
 products
    │ product_id
    ▼
inventory
```

| Table | Rows | Key Columns |
|---|---|---|
| `customers.csv` | 100,200 | customer_id, customer_name, city, area, signup_date |
| `products.csv` | 2,000 | product_id, product_name, category, brand, price |
| `orders.csv` | 1,200,000 | order_id, customer_id, order_date, order_time, store_id, delivery_time_minutes, order_status |
| `order_items.csv` | 3,000,000 | order_item_id, order_id, product_id, quantity, unit_price |
| `inventory.csv` | 6,000,000 | inventory_date, store_id, product_id, stock_quantity |

**Total: 10,302,200 rows**

---

## 🎯 Target Business Metrics

| Category | Metrics |
|---|---|
| 📦 Orders | Total orders, daily orders, cancelled orders, average order value |
| 🛍️ Products | Top-selling products, top categories, revenue by category |
| 📊 Inventory | Stock availability, low-stock products, stock-out rate, demand vs inventory |
| 🚚 Delivery | Average delivery time, late deliveries, delivery performance by location |
| 📍 Location | Orders by area, revenue by area, demand by area |

---

## 🧪 Why Synthetic Data?

Real Instamart/Swiggy transactional data is proprietary and not publicly available — no student or portfolio project can legitimately use it. Instead, this project generates **realistic synthetic data** with deliberately engineered patterns, so the pipeline has genuine signal to surface (rather than flat, random noise):

- **Popularity skew:** Product demand follows a Zipf-like (80/20) distribution — a small set of products drive most order volume.
- **Time patterns:** Orders peak on weekends and evenings (6–9 PM).
- **Store performance variation:** 5 of 50 stores are deliberately slower on delivery (15–30 min worse), producing a realistic "underperforming store" signal.
- **Correlated cancellations:** Order cancellation probability increases with delivery delay, rather than being purely random.
- **Correlated stock-outs:** Popular products are given lower stock levels, mimicking real demand-driven stock-outs.

**Intentional data messiness** (so the Silver-layer cleaning step has genuine work to do, like real pipelines):
- Small % of null values (`customers.area`, `orders.store_id`)
- A handful of duplicate customer rows
- A small % of invalid values (negative `delivery_time_minutes`)

All of this is documented and reproducible — see `generate_data.py`.

---

## 📊 Dashboard Preview

**Page 1 — Overview:** business summary at a glance (KPI cards, daily trend, top products, category revenue)

![Dashboard Overview page](./screenshots/dashboard_page1_overview.png)

**Page 2 — Operations:** operational detail (delivery performance by store, revenue by area, inventory health)

![Dashboard Operations page](./screenshots/dashboard_page2_operations.png)

The full interactive dashboard is available as `instramart_dashboard.pbix` in this repo — open it in Power BI Desktop (free) to explore the data yourself.

---

## ⚙️ Airflow Orchestration Setup (Docker)

The full pipeline — including data quality testing — is automated with **Apache Airflow**, running entirely in **Docker** on the local machine (no cloud). The setup lives in the `airflow/` folder.

**Stack:**
- **Postgres 13** — Airflow's metadata database
- **Apache Airflow 2.9.3** — webserver + scheduler (LocalExecutor, single-machine — sufficient for this project's simple sequential pipeline, so the heavier CeleryExecutor/Redis setup was intentionally skipped)
- **Custom Docker image** — extends the base Airflow image with **Java 17** (required by PySpark) and Python dependencies (`pyspark`, `pyarrow`, `duckdb`, upgraded `pandas>=2.2.0`)

**DAG:** `instramart_pipeline` (`airflow/dags/instramart_pipeline.py`) — a 4-task `BashOperator` chain, manually triggered from the Airflow UI:

```
validate_bronze  →  build_silver  →  build_gold  →  validate_gold
(check_data.py)     (silver_layer.py) (gold_layer.py) (test_gold_layer.py)
```

The final `validate_gold` task runs the data quality test suite (see below) against the freshly-built Gold tables — if any check fails, the task (and the pipeline run) fails, so bad data never silently reaches the dashboard.

**To run it locally:**
```bash
cd airflow
docker compose up -d --build
```
Then open the Airflow UI at `http://localhost:8080` (login: `admin` / `admin`) and trigger the `instramart_pipeline` DAG.

**Issues hit and fixed during setup** (kept here as genuine troubleshooting context, not just a changelog):

| Issue | Fix |
|---|---|
| `docker-compose` command not found | Newer Docker Desktop uses `docker compose` (no hyphen, built-in plugin) instead of the old standalone `docker-compose` |
| Mounted project folder not visible inside containers | Volume mounts only take effect on container **recreation**, not a plain `restart` — required `docker compose up -d --force-recreate` |
| PySpark not usable in the container | Base Airflow image has no Java — added a custom `Dockerfile` that installs OpenJDK 17 and sets `JAVA_HOME` |
| `pip install pyspark` timed out mid-build | Large package (~300MB+) hit pip's default network timeout — added `--default-timeout=120 --retries 5` |
| DAG not appearing in the Airflow UI | The `./dags` volume mount line was accidentally missing from the `airflow-webserver` and `airflow-scheduler` services in `docker-compose.yaml` (only `airflow-init` had it) — added it to all three |
| `403 FORBIDDEN` / "secret_key" error reading task logs | Each Airflow component generates its own random `secret_key` by default; the webserver couldn't authenticate to fetch logs from the scheduler. Fixed by setting a single fixed `AIRFLOW__WEBSERVER__SECRET_KEY` across all three services |
| `check_data.py` task failing: `FileNotFoundError: data/customers.csv` | Script's `DATA_DIR` pointed at `data/` instead of `data/bronze/` — a pre-existing bug, unrelated to Docker, that only surfaced once the task actually ran end-to-end |
| `build_gold` task failing: `PySparkImportError: Pandas >= 2.2.0 required` | The base image's pre-installed pandas was older than what this PySpark version's `.toPandas()` conversion requires — pinned `pandas>=2.2.0` in the custom image |

---

## ✅ Data Quality Testing

`test_gold_layer.py` sanity-checks all 6 Gold-layer tables after every build — either run manually or automatically as the DAG's final task.

**Checks per table:**
1. File exists
2. Row count > 0 (no silently-empty tables)
3. No nulls in key columns (e.g. `product_id`, `product_name`, `store_id`, `city`/`area`)
4. No negative values in numeric columns that should never be negative (e.g. `revenue`, `total_orders`, `total_stock`)

Exits with a non-zero code on failure, so it works as a pass/fail gate — in Airflow, a failed `validate_gold` task marks the whole DAG run as failed.

**Run manually:**
```bash
python test_gold_layer.py
```

---

## 📁 Project Structure

```
instramart-data-pipeline/
├── generate_data.py       # Synthetic data generator (vectorized pandas/numpy)
├── check_data.py          # Bronze-layer data validation & sanity-check script
├── silver_layer.py        # Bronze → Silver PySpark cleaning job
├── gold_layer.py           # Silver → Gold PySpark business metrics job
├── test_gold_layer.py      # Gold-layer data quality tests (Week 8)
├── query_layer.py          # DuckDB query layer answering business questions
├── data/
│   ├── bronze/             # Raw CSVs (source of truth, never edited)
│   ├── silver/              # Cleaned Parquet tables (Week 3)
│   └── gold/                # Business-ready aggregated Parquet tables (Week 4)
├── instramart_dashboard.pbix  # Power BI dashboard (Week 6)
├── screenshots/            # Dashboard preview images (for this README)
├── src/                    # Pipeline scripts (Silver/Gold transformation logic)
├── notebook/               # Exploratory analysis notebooks
├── airflow/                # Airflow orchestration (Week 7)
│   ├── dags/
│   │   └── instramart_pipeline.py   # 4-task DAG: validate_bronze → build_silver → build_gold → validate_gold
│   ├── docker-compose.yaml           # Postgres + Airflow webserver + scheduler
│   └── Dockerfile                    # Custom image: base Airflow + Java 17 + PySpark deps
└── README.md
```

---

## ✅ Progress Log

- [x] **Week 1 — Data Foundation:** Business problem defined, 5-table data model designed, synthetic data generator built (10.3M rows), data validated (zero referential integrity issues)
- [x] **Week 2 — Bronze Layer:** Raw CSVs organized into `data/bronze/` (customers, products, orders, order_items, inventory); `data/silver/` and `data/gold/` folders created and ready
- [x] **Week 3 — Silver Layer:** PySpark cleaning job built and run — nulls handled, duplicates removed, invalid values fixed, all 5 tables written as Parquet to `data/silver/` (see [Local Environment Setup](#-local-environment-setup-windows) for the PySpark/Hadoop-on-Windows configuration this required)
- [x] **Week 4 — Gold Layer:** PySpark business transformations built and run — 6 aggregated tables written to `data/gold/`: `daily_orders`, `product_performance`, `category_performance`, `inventory_health`, `delivery_performance`, `location_insights`
- [x] **Week 5 — Query Layer:** DuckDB script built and run against the full 10.3M-row dataset — answers every business question from Step 2 directly via SQL on the Gold-layer Parquet files (orders, products, inventory, delivery, location)
- [x] **Week 6 — Dashboard:** Power BI dashboard built (`instramart_dashboard.pbix`) with 2 pages: **Overview** (3 KPI cards, daily orders trend, top products, category revenue) and **Operations** (delivery performance by store, revenue by area, inventory health table). Dark theme applied for a polished, professional look.
- [x] **Week 7 — Orchestration:** Apache Airflow deployed via Docker Compose (Postgres metadata DB + webserver + scheduler, custom image with Java 17 + PySpark). A DAG automates the full Bronze → Silver → Gold pipeline with a single click from the Airflow UI. See [Airflow Orchestration Setup](#️-airflow-orchestration-setup-docker) above for the local Docker configuration this required.
- [x] **Week 8 — Testing & Polish:** Gold-layer data quality test suite (`test_gold_layer.py`) built and wired into the DAG as a 4th task (`validate_gold`), so every pipeline run is self-validating. Documentation finalized; `v1.0` released.

---

## 🛠️ Tech Stack

- **Language:** Python 3.13
- **Data generation/validation:** pandas, numpy
- **Distributed processing:** PySpark (Java 17 / Eclipse Temurin runtime)
- **Query engine:** DuckDB
- **File format:** Parquet (Silver/Gold layers)
- **Dashboarding:** Power BI Desktop
- **Orchestration:** Apache Airflow (Docker), Postgres (metadata DB)
- **Testing:** Custom pandas-based data quality checks
- **Version control:** Git / GitHub

---

## 🚀 Getting Started

```bash
# 1. Clone the repo
git clone <repo-url>
cd instramart-data-pipeline

# 2. Install dependencies
pip install pandas numpy pyspark duckdb pyarrow

# 3. Generate the synthetic dataset
python generate_data.py

# 4. Validate the raw data
python check_data.py

# 5. (Optional) Run the full pipeline via Airflow instead of manually
cd airflow
docker compose up -d --build
# then trigger the "instramart_pipeline" DAG at http://localhost:8080

# 6. (Optional) Run Gold-layer data quality tests manually
python test_gold_layer.py
```

> **Note:** PySpark requires Java 17 (JDK) installed and `JAVA_HOME` set. See [Adoptium](https://adoptium.net) for the JDK installer.

---

## 🪟 Local Environment Setup (Windows)

Running PySpark locally on Windows required additional configuration beyond a plain `pip install`, since PySpark depends on Hadoop libraries that were originally built for Linux. The full diagnosis and fix for each issue is documented in [`TROUBLESHOOTING_JOURNAL.md`](./TROUBLESHOOTING_JOURNAL.md) — summary below:

| Issue | Fix |
|---|---|
| Wrong JDK version installed | Use JDK 17 specifically (LTS), not the latest release |
| `java` command not recognized | Set `JAVA_HOME` and add `%JAVA_HOME%\bin` to `Path` |
| PySpark startup hangs | Force `spark.driver.host=localhost` in SparkSession config |
| `winutils.exe` missing (read errors) | Download from [cdarlint/winutils](https://github.com/cdarlint/winutils), set `HADOOP_HOME` |
| Fatal error writing Parquet files | Also requires `hadoop.dll`, placed in both `HADOOP_HOME\bin` and `C:\Windows\System32` |
| Windows blocks downloaded `.exe`/`.dll` | Right-click → Properties → check "Unblock" |
| Power BI can't open Gold-layer Parquet ("path is a folder path" error) | Spark writes Parquet as a folder of part-files, not a single file — Gold-layer tables are small enough to write via pandas/pyarrow instead, producing genuine single `.parquet` files that Power BI/Excel can open directly |

See the full journal for root-cause explanations of each — this is genuinely useful context for anyone else setting up PySpark on Windows, not just a record of what went wrong.

---

## 📐 Design Decisions

- **Why local instead of cloud (AWS)?** To avoid billing risk while learning, this project uses local, free tools that mirror AWS's architecture 1:1 (see the Architecture section above). All core data engineering concepts — distributed processing, medallion architecture, SQL analytics, orchestration — transfer directly to a cloud deployment.
- **Why Parquet over CSV for Silver/Gold?** Parquet is columnar, compressed, and much faster to read/write at scale than CSV — the same reason real pipelines avoid CSV beyond the raw ingestion layer.
- **Why inject messiness into the data?** Real-world data is never perfectly clean. Deliberately including a small, controlled amount of nulls/duplicates/invalid values gives the Silver-layer cleaning step genuine, demonstrable work — rather than a trivial pass-through.
- **Why LocalExecutor instead of CeleryExecutor for Airflow?** This pipeline is a simple sequential chain running on a single machine — LocalExecutor handles that fine without the added complexity of Redis/Celery workers, which only pays off for distributed, multi-worker task execution.
- **Why data quality tests as a DAG task instead of a separate manual step?** Real pipelines treat validation as part of the pipeline itself, not an optional afterthought — wiring it in as `validate_gold` means a broken Gold layer fails the whole run automatically, instead of silently reaching the dashboard.

---

## 📄 License

This is a personal learning/portfolio project. Synthetic data only — no real company data is used or represented.