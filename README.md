# 🏗️ Econ Data Pipeline

> **Production-grade macroeconomic data pipeline** ingesting data from the Bureau of Economic Analysis (BEA), FRED, and BLS — transforming it through a multi-layer lakehouse and orchestrating workflows with Apache Airflow, Mage, and Kestra.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![dlt](https://img.shields.io/badge/dlt-0.4-orange)
![dbt](https://img.shields.io/badge/dbt-1.7-red?logo=dbt)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-003366?logo=databricks)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-017CEE?logo=apache-airflow)
![Mage](https://img.shields.io/badge/Mage.ai-0.9-7C3AED)
![Kestra](https://img.shields.io/badge/Kestra-0.14-blueviolet)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10-FFF000?logo=duckdb)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?logo=google-cloud)
![Terraform](https://img.shields.io/badge/Terraform-1.6-7B42BC?logo=terraform)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazon-aws)
![CI/CD](https://img.shields.io/badge/GitHub_Actions-CI%2FCD-2088FF?logo=github-actions)

---

## 📐 Architecture

```
               ┌─────────────────────────────────────────────────────┐
               │                      DATA SOURCES                   │
               │  ┌──────────────────┐ ┌──────────┐   ┌──────────┐   │
               │  │   BEA (primary)  │ │   FRED   │   │   BLS    │   │
               │  │ GDP · PCE · ITA  │ │CPI · M2  │   │PPI·Wages │   │
               │  │ Income · Profits │ │Fed Funds │   │Employment│   │
               │  └────────┬─────────┘ └────┬─────┘   └────┬─────┘   │
               └───────────┼────────────────┼──────────────┼─────────┘
                           └────────────────┴──────────────┘
                                            │
                                ┌───────────▼───────────┐
                                │    dlt  (ingestion)   │
                                │  schema inference     │
                                │  incremental loads    │
                                │  retry + back-off     │
                                └─────────┬─────────────┘
                                          │
                     ┌────────────────────┼────────────────────┐
                     ▼                    ▼                    ▼
              ┌────────────┐      ┌──────────────┐    ┌──────────────┐
              │   AWS S3   │      │  PostgreSQL  │    │  BigQuery    │
              │ + Delta    │      │  (OLTP/dev)  │    │  (OLAP/prod) │
              │   Lake     │      │  Docker + CI │    │  analytical  │
              │ (lakehouse)│      └──────┬───────┘    └──────┬───────┘
              └─────┬──────┘             └──────┬────────────┘
                    │                           │
                    └──────────────┬────────────┘
                                   │
                       ┌───────────▼───────────┐
                       │   dbt transformations │
                       │  raw → staging        │
                       │  staging → marts      │
                       │  fct_macro_indicators │
                       │  (ML feature table)   │
                       └───────────┬───────────┘
                                   │
                       ┌───────────▼───────────┐
                       │  DuckDB analytics     │
                       │  (local dev queries   │
                       │   directly on Delta)  │
                       └───────────┬───────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
    ┌──────────────────┐  ┌──────────────┐   ┌──────────────────┐
    │  Apache Airflow  │  │   Mage.ai    │   │     Kestra       │
    │  (battle-tested) │  │  (modern DX) │   │  (YAML-first,    │
    │  Python DAGs     │  │  notebooks   │   │  event-driven)   │
    │  Mon-Fri daily   │  │  data preview│   │  webhook trigger │
    └──────────────────┘  └──────────────┘   └──────────────────┘
                                   │
                       ┌───────────▼───────────┐
                       │   Downstream stack    │
                       │  econ-forecast-engine │
                       │  econ-streaming-pipe  │
                       │  econ-ml-platform     │
                       │  econ-forecaster-dash │
                       └───────────────────────┘
```

---

## 💡 Design Decisions

**Why BEA as the primary GDP/PCE source instead of FRED?**
BEA is the *publisher of record* for GDP and PCE. FRED republishes BEA data with a lag at lower granularity. Going direct means earlier access to revisions and PCE broken down into monthly sub-categories (healthcare, housing, durables, services) that simply don't exist in FRED.

**Why Delta Lake on top of S3?**
Raw Parquet on S3 is just files — no ACID guarantees, no upsert semantics, no audit trail. Economic data gets revised constantly (BEA revises GDP quarterly). Delta Lake adds MERGE for upserts, full transaction history for time travel, and schema enforcement — all without leaving S3. Critically, these same Delta tables mount directly into Databricks via Unity Catalog with zero ETL if the stack moves there.

**Why three orchestrators (Airflow + Mage + Kestra)?**
Not because you'd run all three in production, but because the orchestration landscape is genuinely fragmented right now and demonstrating awareness of the tradeoffs matters in interviews. Airflow is still the enterprise default. Mage is gaining adoption at startups for its notebook-style DX. Kestra is emerging as the YAML-first, language-agnostic choice for polyglot teams.

**Why DuckDB alongside PostgreSQL?**
PostgreSQL is the standard OLTP store and the CI database. DuckDB is an in-process analytical engine that reads the Delta Lake Parquet files directly — no ETL, no server, and 10-100x faster than Postgres for column scan queries. It also serves as the fastest dbt development target (no Docker needed, instant startup).

**Why dlt for ingestion instead of custom ETL?**
dlt handles schema inference, type coercion, incremental load state, and multi-destination writes out of the box — eliminating ~300 lines of boilerplate per pipeline. Adding a new source is a single `@dlt.resource`-decorated generator function.

---

## 📁 Project Structure

```
econ-data-pipeline/
├── ingestion/
│   ├── base_pipeline.py           # Base class: S3 archival, retry, dlt wiring
│   ├── bea_pipeline.py            # BEA NIPA: GDP, PCE, income, trade (PRIMARY)
│   ├── fred_pipeline.py           # FRED: CPI, Fed Funds, M2, unemployment, yields
│   └── bls_pipeline.py            # BLS: payrolls, unemployment, ECI, CPI, productivity
│
├── delta_lake/
│   ├── delta_writer.py            # PySpark + Delta Lake: MERGE upserts, time travel
│   └── databricks_notebook.py    # Databricks-compatible analysis notebook
│
├── duckdb/
│   └── duckdb_analytics.py       # In-process analytics on Delta files + benchmark
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml               # DuckDB (dev) + Postgres (dev) + BigQuery (prod)
│   └── models/
│       ├── staging/
│       │   ├── stg_bea_observations.sql
│       │   ├── stg_fred_observations.sql
│       │   ├── sources.yml
│       │   └── schema.yml
│       └── marts/forecasting/
│           └── fct_macro_indicators_monthly.sql
│
├── airflow/dags/
│   ├── dag_daily_macro_pipeline.py     # Mon-Fri: daily updates + dbt
│   ├── dag_weekly_fred_pipeline.py     # Every Monday: FRED indicators + dbt
│   └── dag_monthly_bea_pipeline.py     # 3rd of month: BEA NIPA + Delta merge + dbt full-refresh
│
├── mage/pipelines/
│   └── macro_ingestion_pipeline.py    # Mage block-based pipeline (alternative)
│
├── kestra/flows/
│   └── macro_ingestion_pipeline.yml   # Kestra YAML flow (alternative)
│
├── terraform/
│   ├── main.tf                    # AWS S3 + GCP BigQuery + IAM
│   ├── variables.tf               # Variable declarations (aws_region, gcp_project_id, etc.)
│   └── outputs.tf                 # Bucket names, service account email, IAM ARN
│
├── docker/
│   ├── docker-compose.yml         # PostgreSQL + Airflow + pgAdmin
│   ├── Dockerfile.pipeline
│   └── scripts/
│       └── init_db.sql            # PostgreSQL schema init (raw/staging/marts + stub tables)
│
├── tests/
│   ├── test_bea_pipeline.py
│   └── test_fred_pipeline.py
├── .github/workflows/ci.yml
├── .env.example
└── requirements.txt
```

---

## 🔧 Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | **dlt 0.4** | Schema-inferred pipeline loading with incremental state |
| Primary Source | **BEA API** | GDP, PCE sub-categories, national income, trade |
| Secondary Sources | **FRED · BLS** | Monetary, labor |
| Raw Storage | **AWS S3** | Partitioned raw data lake |
| Lakehouse | **Delta Lake 3.0** | ACID transactions, MERGE upserts, time travel on S3 |
| Notebook Compute | **Databricks** | Delta table consumption, Spark SQL, ML feature export |
| Local Analytics | **DuckDB 0.10** | In-process OLAP on Delta Parquet; dbt dev target |
| OLTP | **PostgreSQL 15** | Docker dev + CI test database |
| OLAP | **BigQuery** | Production analytical warehouse |
| DataFrame | **Pandas + Polars** | Data manipulation (Polars for performance-sensitive paths) |
| Transform | **dbt 1.7** | Staging → intermediate → marts modeling |
| Orchestration | **Apache Airflow 2.8** | Production DAGs, freshness checks, Slack alerts |
| Orchestration (alt) | **Mage.ai** | Notebook-style blocks, built-in data preview |
| Orchestration (alt) | **Kestra** | YAML-first, event-driven, webhook triggers |
| Infrastructure | **Terraform 1.6** | IaC for S3, BigQuery, IAM roles, service accounts |
| Containerization | **Docker Compose** | Local: Postgres + Airflow + pgAdmin |
| CI/CD | **GitHub Actions** | Lint → tests → dbt compile → docker build → tf validate |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Java 11+ (for local PySpark/Delta Lake)
- Terraform 1.6+
- AWS CLI configured
- GCP credentials

### 1. Clone & configure
```bash
git clone https://github.com/2Pure1/econ-data-pipeline.git
cd econ-data-pipeline
cp .env.example .env
# Fill in API keys — see API Keys section
```

### 2. Start local infrastructure
```bash
docker compose -f docker/docker-compose.yml up -d
# PostgreSQL :5432  ·  Airflow :8080  ·  pgAdmin :5050
```

### 3. Provision cloud resources
```bash
cd terraform
terraform init
terraform plan -var="gcp_project_id=YOUR_PROJECT"
terraform apply
```

### 4. Run ingestion pipelines
```bash
pip install -r requirements.txt

python ingestion/bea_pipeline.py --destination postgres   # BEA first (primary source)
python ingestion/fred_pipeline.py --destination postgres
python ingestion/bls_pipeline.py --destination postgres

# Explore available BEA tables
python ingestion/bea_pipeline.py --list-tables
```

### 5. Write to Delta Lake
```bash
# Upsert all tables to Delta Lake (handles data revisions via MERGE)
python delta_lake/delta_writer.py --table all --mode merge

# Run OPTIMIZE + Z-ORDER after a bulk load
python delta_lake/delta_writer.py --table macro_indicators_monthly --optimize

# Inspect Delta history (time travel audit log)
python delta_lake/delta_writer.py --table fred_observations --history
```

### 6. Query with DuckDB
```bash
# Ad-hoc analytics directly on Delta Parquet files (no ETL, instant)
python duckdb/duckdb_analytics.py --query "SELECT year, AVG(unemployment_rate) FROM macro_monthly GROUP BY year ORDER BY year"

# Recession period analysis
python duckdb/duckdb_analytics.py --recession

# Benchmark DuckDB vs PostgreSQL
python duckdb/duckdb_analytics.py --benchmark
```

### 7. Run dbt (three targets)
```bash
cd dbt_project

# DuckDB — fastest local dev (no Docker needed)
dbt run --target duckdb

# PostgreSQL — standard dev with Docker
dbt run --target dev

# BigQuery — production
dbt run --target prod_bigquery

dbt test
dbt docs generate && dbt docs serve
```

### 8. Alternative orchestration

**Mage.ai:**
```bash
pip install mage-ai
mage start econ-data-pipeline
# Open http://localhost:6789 → import mage/pipelines/
```

**Kestra:**
```bash
docker run --pull=always --rm -it -p 8080:8080 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp kestra/kestra:latest server local
# Open http://localhost:8080 → import kestra/flows/
```

### 9. Access Airflow
```
http://localhost:8080  →  admin / admin
```

---

## 🔑 API Keys

| Source | Required | Cost | Link |
|--------|---------|------|------|
| **BEA** | ✅ Yes | Free | [apps.bea.gov/api/signup](https://apps.bea.gov/api/signup/) |
| **FRED** | ✅ Yes | Free | [fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html) |
| **BLS** | ⚠️ Recommended | Free | [bls.gov/developers](https://www.bls.gov/developers/api_signature_v2.htm) |

> BLS works without a key but is rate-limited to 25 requests/day and 10 years of history. A registered key raises that to 500/day and 20 years.

---

## 📊 Data Sources & Indicators

### BEA — National Income and Product Accounts (Primary)

| NIPA Table | Indicators | Frequency |
|-----------|-----------|-----------|
| T10105 — GDP & Components | GDP, PCE, Gross Investment, Net Exports, Gov't Spending | Quarterly |
| T10101 — Real GDP Growth | Real GDP % change, Real PCE % change | Quarterly |
| T10104 — Price Indexes | GDP deflator, PCE price index, Import/Export prices | Quarterly |
| T20305 — PCE by Category | Goods, Durables, Nondurables, Services, Healthcare, Housing | **Monthly** |
| T20100 — Personal Income | Personal income, Wages & salaries, Saving rate | Monthly |
| T61600A — Corporate Profits | Total, Financial, Nonfinancial sector profits | Quarterly |
| T40100 — International Transactions | Current account, Goods/Services trade flows | Quarterly |

### FRED (Secondary)

| Indicators | Frequency |
|-----------|-----------|
| CPI all urban + Core CPI | Monthly |
| Fed Funds Rate, 10Y/2Y Treasury yields, Yield curve spread (T10Y2Y) | Daily/Monthly |
| M2 Money Supply | Monthly |
| Unemployment (U-3 + U-6), Nonfarm Payrolls, Labor Force Participation | Monthly |
| Consumer Sentiment (UMich), Industrial Production, Housing Starts | Monthly |

### BLS — Bureau of Labor Statistics

| Series | Description | Frequency |
|--------|------------|-----------|
| CES0000000001 | Total Nonfarm Payrolls | Monthly |
| LNS14000000 | Unemployment Rate (U-3) | Monthly |
| LNS13327709 | U-6 Underemployment Rate | Monthly |
| CES0500000003 | Average Hourly Earnings, Private | Monthly |
| CIU1010000000000A | Employment Cost Index | Quarterly |
| CUSR0000SA0 | CPI-U All Items (seasonally adjusted) | Monthly |
| CUSR0000SA0L1E | Core CPI ex Food & Energy | Monthly |
| PRS85006092 | Nonfarm Business Productivity | Quarterly |



---

## 🏛️ dbt Data Model

```
raw schema  (loaded by dlt)
  ├── bea_nipa_observations
  └── fred_observations

staging schema  (dbt views — cleaned, typed, deduplicated)
  ├── stg_bea_observations
  └── stg_fred_observations

marts.forecasting  (dbt tables)
  └── fct_macro_indicators_monthly
        ~420 rows (1990–present), ~50 columns.
        Primary ML training feature table.

        BEA:     gdp_billions · real_gdp_qoq_pct · pce_total
                 pce_goods/services/durables/healthcare/housing
                 personal_income · wages_salaries · saving_rate
                 current_account · goods/services trade flows

        FRED:    fed_funds_rate · m2_money_supply · yield_curve_spread
                 cpi_all_urban · core_cpi · unemployment_rate
                 nonfarm_payrolls · housing_starts

        Derived: cpi_yoy_pct · core_cpi_yoy_pct
                 real_fed_funds_rate
                 yield_curve_inverted (flag)
                 payrolls_mom_change_thousands
                 pce_services_goods_ratio
                 gdp_output_gap_proxy
```

---

## 🦅 Delta Lake Features

| Feature | Purpose in this project |
|---------|------------------------|
| **ACID MERGE** | Upserts revised BEA/FRED data without full reloads |
| **Time travel** | Read the feature table `AS OF version N` — eliminates look-ahead bias in ML backtests |
| **Schema evolution** | Add new BEA series without breaking existing consumers |
| **OPTIMIZE + Z-ORDER** | Compact small files; co-locate by date for fast time-range queries |
| **Streaming reads** | Delta tables can be consumed as a Spark Structured Streaming source — connects to `econ-streaming-pipeline` |
| **Databricks compatible** | Mount these S3 Delta tables in Databricks Unity Catalog with zero ETL |

---

## 🎛️ Orchestration Comparison

| Feature | Airflow 2.8 | Mage.ai | Kestra |
|---------|------------|---------|--------|
| Config style | Python DAGs | Python blocks | YAML flows |
| Local dev | Moderate (Docker) | Easy (pip install) | Easy (Docker) |
| Data preview | ❌ | ✅ Built-in | ❌ |
| dbt integration | Plugin | Native | Plugin |
| Event triggers | Limited | Limited | ✅ Native webhooks |
| Best for | Complex graphs, large teams | Fast iteration, small teams | Polyglot, event-driven |
| Industry status | Enterprise default | Growing adoption | Emerging |

All three orchestrators run the same underlying pipeline logic — the ingestion, Delta write, and dbt refresh steps are identical. The orchestrator is swappable.

---

## 🧪 Data Quality

- `unique` + `not_null` on all surrogate keys
- `accepted_values` for category and frequency enum fields
- Source freshness: warn at 2 days stale, error at 7 days
- Deduplication via `row_number()` in all staging models
- `is_complete_record` boolean flag on the mart for ML filtering
- Look-ahead bias protection via Delta Lake time travel versioning

---

## ☁️ Cloud Infrastructure (Terraform)

**AWS**
- S3 raw bucket — versioned, AES-256 encrypted, lifecycle: Standard-IA at 30d → Glacier at 90d
- S3 processed bucket — versioned
- IAM pipeline user with least-privilege policy scoped to pipeline buckets

**GCP**
- BigQuery datasets: `econ_warehouse`, `econ_staging`, `econ_marts`
- Service account with `bigquery.dataEditor` + `bigquery.jobUser` roles
- Service account key exported as a sensitive Terraform output

---

## 🗺️ Roadmap

- [ ] **Apache Iceberg** — evaluate as alternative open table format to Delta Lake
- [ ] **Polars** migration for ingestion-layer DataFrame ops (in progress via `requirements.txt`)
- [ ] **Databricks Community Edition** deployment guide
- [ ] **Great Expectations** data quality suite (beyond dbt tests)
- [ ] **dlt + Motherduck** (DuckDB cloud) as a lightweight production alternative to BigQuery

---

## 📈 Downstream Projects

| Project | Consumes |
|---------|---------|
| [`econ-forecast-engine`](https://github.com/2Pure1/econ-forecast-engine) | `fct_macro_indicators_monthly` as ML training features |
| [`econ-streaming-pipeline`](https://github.com/2Pure1/econ-streaming-pipeline) | Delta Lake streaming reads + Kafka + Spark |
| [`econ-ml-platform`](https://github.com/2Pure1/econ-ml-platform) | FastAPI + TF Serving model inference |
| [`econ-forecaster-dashboard`](https://github.com/2Pure1/econ-forecaster-dashboard) | Live forecast visualisation from marts |

---

## 📄 License

MIT
