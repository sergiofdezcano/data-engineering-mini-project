# Data Engineering Mini Project

> **A production-ready ETL pipeline for cryptocurrency market data using the most in-demand Modern Data Stack tools**

This project demonstrates a complete data engineering workflow from extracting data from a public API, through modular SQL transformations, to generating analytics-ready tables for visualization or reporting.

[![Language: Spanish](https://img.shields.io/badge/Idioma-Español-blue)](./README_ES.md)

## Table of Contents

- [What Does This Project Do?](#what-does-this-project-do)
- [Demonstrated Concepts](#demonstrated-concepts)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Installation and Usage](#installation-and-usage)
- [Project Structure](#project-structure)
- [Detailed Pipeline](#detailed-pipeline)
- [Data Models](#data-models)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
- [Additional Resources](#additional-resources)

---

## What Does This Project Do?

This project implements an **automated ETL pipeline** that:

1. **Extracts** data from the top 50 cryptocurrencies via CoinGecko API
2. **Loads** raw data into a PostgreSQL database
3. **Transforms** the data using dbt in layers (staging → marts)
4. **Validates** data quality with automated tests
5. **Documents** data lineage and schema automatically
6. **Orchestrates** the entire process with Apache Airflow every 6 hours

**Result**: Analytics-ready tables for dashboards or reports with metrics like volatility, market cap categories, performance, etc.

---

## Demonstrated Concepts

This project is ideal for portfolios because it demonstrates knowledge in:

### Data Engineering
- ✅ **ETL Pipeline Design**: Clear separation of Extract, Load, Transform
- ✅ **Data Modeling**: Medallion Architecture (Raw → Staging → Marts)
- ✅ **Data Quality**: Automated tests with dbt
- ✅ **Orchestration**: Complex workflows with Airflow DAGs
- ✅ **SQL Analytics**: SQL transformations with CTEs, CASE statements, aggregations

### DevOps & Infrastructure
- ✅ **Infrastructure as Code**: Docker Compose for the entire stack
- ✅ **Containerization**: Multi-container orchestration
- ✅ **Environment Management**: Environment variables and configuration
- ✅ **Reproducibility**: Single command to spin up the entire system

### Best Practices
- ✅ **Version Control**: Git-friendly structure
- ✅ **Documentation**: Comprehensive README + automatic dbt docs
- ✅ **Modularity**: Organized and reusable code
- ✅ **Scalability**: Easy to extend with new data sources
- ✅ **Monitoring**: Structured logs and healthchecks

---

## Tech Stack

| Technology | Purpose | Why This Tool? |
|------------|---------|----------------|
| **Apache Airflow** | Workflow orchestration | Industry standard for scheduling and monitoring data pipelines. Used by Airbnb, Twitter, Reddit |
| **dbt (data build tool)** | SQL transformations | The #1 tool for Analytics Engineers. Enables testing, documentation, and version control of SQL transformations |
| **PostgreSQL** | Data Warehouse | Robust open-source relational database. Easy to scale to Redshift, BigQuery, or Snowflake |
| **Docker Compose** | Container orchestration | Allows replicating the entire dev/production environment on any machine with a single command |
| **Python 3.11** | Scripting and processing | Standard language in Data Engineering. Used for extraction, processing, and orchestration |
| **CoinGecko API** | Data source | Free public API with real market data. Easy to replace with other APIs |

## Architecture

### Architecture Diagram

```
                    ┌─────────────────────────────────────────┐
                    │         DOCKER COMPOSE                  │
                    │                                         │
┌─────────────┐     │  ┌──────────────────────────────────┐  │
│             │     │  │      APACHE AIRFLOW              │  │
│  CoinGecko  │────────▶│                                  │  │
│     API     │ HTTP  │  │  ┌─────────┐   ┌────────────┐ │  │
│             │     │  │  │ Webserver│   │  Scheduler │ │  │
└─────────────┘     │  │  └─────────┘   └────────────┘ │  │
                    │  │                                  │  │
   (Real market     │  │  DAG: crypto_etl_pipeline        │  │
     data)          │  └────────────┬─────────────────────┘  │
                    │               │                        │
                    │               ▼                        │
                    │  ┌──────────────────────────────────┐  │
                    │  │       POSTGRESQL                 │  │
                    │  │                                  │  │
                    │  │  Table: raw_crypto_data          │  │
                    │  │  (Unprocessed data)              │  │
                    │  └────────────┬─────────────────────┘  │
                    │               │                        │
                    │               ▼                        │
                    │  ┌──────────────────────────────────┐  │
                    │  │         DBT CORE                 │  │
                    │  │                                  │  │
                    │  │  Staging Layer:                  │  │
                    │  │  └─ stg_crypto_data (VIEW)       │  │
                    │  │                                  │  │
                    │  │  Marts Layer:                    │  │
                    │  │  └─ crypto_market_summary (TABLE)│  │
                    │  │                                  │  │
                    │  │  + Tests + Documentation         │  │
                    │  └──────────────────────────────────┘  │
                    │                                         │
                    └─────────────────────────────────────────┘

           Everything runs on your local machine with one command
```

### Detailed Data Flow

#### 1️⃣ **EXTRACT** (Python Script)
- **File**: `scripts/extract_data.py`
- **Action**: Makes GET request to CoinGecko API
- **Data obtained**: Top 50 cryptos with price, market cap, volume, 24h changes, etc.
- **Format**: JSON stored in `data/raw/crypto_data_YYYYMMDD_HHMMSS.json`
- **Frequency**: Every 6 hours (configurable)

#### 2️⃣ **LOAD** (Python + SQLAlchemy)
- **File**: `scripts/extract_data.py` (`load_to_postgres` function)
- **Action**: Reads JSON and loads to PostgreSQL
- **Target table**: `raw_crypto_data` (public schema)
- **Strategy**: Append (historical records accumulate)

#### 3️⃣ **TRANSFORM** (dbt)
- **Staging**: `stg_crypto_data` (VIEW)
  - Data cleaning (nulls → 0, uppercase symbols)
  - Column renaming for clarity
  - Timestamp parsing
  - **Materialization**: VIEW (no storage, always up-to-date)

- **Marts**: `crypto_market_summary` (TABLE)
  - Analytical metrics calculations:
    - Volatility category (High/Medium/Low)
    - Market cap category (Large/Mid/Small)
    - Performance indicator (Strong Gain/Gain/Loss/Strong Loss)
    - Volume/market cap ratio (liquidity indicator)
  - **Materialization**: TABLE (optimized for analytical queries)

#### 4️⃣ **TEST** (dbt tests)
- Uniqueness tests (crypto_id must be unique)
- NOT NULL tests (critical fields cannot be null)
- Custom tests defined in `schema.yml`
- **Result**: Pipeline fails if data doesn't pass tests

#### 5️⃣ **DOCUMENT** (dbt docs)
- Generates interactive HTML documentation
- Shows lineage (which table depends on what)
- Description of each column
- SQL code of each model

#### 6️⃣ **ORCHESTRATE** (Airflow DAG)
- **File**: `airflow/dags/crypto_etl_pipeline.py`
- **Dependencies**: `extract → load → dbt_run → dbt_test → dbt_docs`
- **Retry logic**: 2 retries with 5-minute wait
- **Alerts**: Configurable via email (currently disabled)

---

## Project Structure

```
data-engineering-mini-project/
├── README.md                   # This file (English)
├── README_ES.md                # Spanish version
├── docker-compose.yml          # Docker services configuration
├── .env.example                # Environment variables template
├── .gitignore
│
├── airflow/
│   ├── dags/
│   │   └── crypto_etl_pipeline.py   # Main ETL DAG
│   ├── plugins/
│   └── requirements.txt        # Python dependencies
│
├── dbt/
│   ├── dbt_project.yml         # dbt configuration
│   ├── profiles.yml            # Database connection
│   └── models/
│       ├── staging/
│       │   ├── stg_crypto_data.sql
│       │   └── schema.yml
│       └── marts/
│           ├── crypto_market_summary.sql
│           └── schema.yml
│
├── scripts/
│   └── extract_data.py         # Data extraction script
│
└── data/
    └── raw/                    # Raw data storage
```

## Installation and Usage

### Prerequisites

Before starting, make sure you have installed:

| Software | Minimum Version | Download Link |
|----------|----------------|---------------|
| Docker Desktop | 20.10+ | [docker.com](https://www.docker.com/products/docker-desktop) |
| Docker Compose | 2.0+ | Included in Docker Desktop |
| Git | 2.0+ | [git-scm.com](https://git-scm.com/) |

**Recommended system resources**:
- 💾 RAM: Minimum 4GB available for Docker
- 💿 Disk: ~5GB free space
- 🖥️ CPU: 2 cores minimum

---

### 🚀 Step-by-Step Installation

#### **Step 1: Clone the repository**

```bash
git clone https://github.com/sergiofdezcano/data-engineering-mini-project.git
cd data-engineering-mini-project
```

#### **Step 2: Configure environment variables**

```bash
# The .env file is already included with default values
# If you want to customize it:
nano .env  # or use your favorite editor

# .env contents:
# POSTGRES_USER=airflow
# POSTGRES_PASSWORD=airflow
# POSTGRES_DB=data_warehouse
```

> 💡 **Note**: For production, change the default credentials

#### **Step 3: Start the services**

```bash
# Start all containers in background
docker-compose up -d

# View startup progress
docker-compose logs -f

# Wait ~2 minutes until you see: "Airflow webserver is ready"
```

**Verify everything is running**:
```bash
docker-compose ps

# You should see 4 containers running:
# - de_postgres          (PostgreSQL database)
# - de_airflow_webserver (Airflow UI)
# - de_airflow_scheduler (Airflow worker)
# - de_airflow_init      (Completed - initialization)
```

#### **Step 4: Access Airflow UI**

1. Open your browser at: **http://localhost:8080**
2. Enter credentials:
   ```
   Username: admin
   Password: admin
   ```
3. You'll see the Airflow dashboard with the `crypto_etl_pipeline` DAG

#### **Step 5: Run the pipeline**

**Option A: Manual execution (recommended for first time)**
1. In Airflow UI, locate the `crypto_etl_pipeline` DAG
2. Activate it using the toggle (OFF → ON)
3. Click the "▶️ Trigger DAG" button (top right)
4. Go to "Graph" view to monitor real-time progress

**Option B: Let it run automatically**
- DAG is configured to run **every 6 hours**
- First execution: When you activate it
- Following: 00:00, 06:00, 12:00, 18:00 UTC

#### **Step 6: Verify results**

**Connect to the database**:
```bash
# Option 1: From terminal
docker exec -it de_postgres psql -U airflow -d data_warehouse

# Option 2: Use a SQL client (DBeaver, pgAdmin, etc.)
# Host: localhost
# Port: 5432
# Database: data_warehouse
# User: airflow
# Password: airflow
```

**Example queries**:
```sql
-- View raw data
SELECT id, symbol, name, current_price, market_cap_rank
FROM raw_crypto_data
LIMIT 10;

-- View transformed data (staging)
SELECT crypto_id, symbol, crypto_name, current_price
FROM stg_crypto_data
LIMIT 10;

-- View final analytical table (marts)
SELECT
    crypto_name,
    symbol,
    current_price,
    market_cap_category,
    volatility_24h,
    performance_24h
FROM marts.crypto_market_summary
ORDER BY market_cap_rank
LIMIT 10;

-- Top 5 most volatile cryptos
SELECT crypto_name, volatility_24h, price_change_24h_pct
FROM marts.crypto_market_summary
WHERE volatility_24h = 'High'
ORDER BY ABS(price_change_24h_pct) DESC
LIMIT 5;
```

---

### 🔄 Useful Commands

```bash
# View logs in real-time
docker-compose logs -f airflow-scheduler

# Restart services
docker-compose restart

# Stop everything
docker-compose down

# Stop and delete ALL data (⚠️ careful!)
docker-compose down -v

# View resource usage
docker stats

# Access a container
docker exec -it de_airflow_scheduler bash
```

### ⏰ Pipeline Scheduling

**Current configuration**: Cron expression `0 */6 * * *`
- Runs every 6 hours: 00:00, 06:00, 12:00, 18:00 UTC
- Configurable in `airflow/dags/crypto_etl_pipeline.py`

**Other schedule options**:
```python
schedule_interval='@hourly'       # Every hour
schedule_interval='@daily'        # Daily at midnight
schedule_interval='0 9 * * *'     # Daily at 9 AM
schedule_interval='*/30 * * * *'  # Every 30 minutes
schedule_interval=None            # Manual only
```

---

## Data Models

This project follows the **Medallion Architecture** (Bronze → Silver → Gold), also known as layered data architecture.

### 🥉 RAW Layer (Bronze Layer)

**Table**: `raw_crypto_data`
- **Type**: Table (PostgreSQL)
- **Schema**: `public`
- **Purpose**: Store raw data as it comes from the API
- **Strategy**: Append-only (complete history)

**Main columns**:
```sql
id                     VARCHAR    -- Unique identifier (e.g., 'bitcoin')
symbol                 VARCHAR    -- Symbol (e.g., 'btc')
name                   VARCHAR    -- Full name
current_price          FLOAT      -- Current price in USD
market_cap             BIGINT     -- Market capitalization
market_cap_rank        INTEGER    -- Rank by market cap
total_volume           BIGINT     -- 24h trading volume
high_24h               FLOAT      -- 24h high price
low_24h                FLOAT      -- 24h low price
price_change_24h       FLOAT      -- 24h price change (USD)
price_change_percentage_24h FLOAT -- 24h price change (%)
circulating_supply     BIGINT     -- Circulating supply
total_supply           BIGINT     -- Total supply
max_supply             BIGINT     -- Maximum supply
ath                    FLOAT      -- All-time high price
atl                    FLOAT      -- All-time low price
last_updated           TIMESTAMP  -- Last API update
extracted_at           TIMESTAMP  -- Extraction timestamp
```

---

### 🥈 STAGING Layer (Silver Layer)

**Model**: `stg_crypto_data`
- **Type**: VIEW (recalculated on each query)
- **Schema**: `public`
- **Purpose**: Clean and standardize raw data
- **File**: `dbt/models/staging/stg_crypto_data.sql`

**Applied transformations**:
- ✅ Column renaming for clarity (`id` → `crypto_id`)
- ✅ Symbol conversion to UPPERCASE (`btc` → `BTC`)
- ✅ Null handling (`COALESCE(fully_diluted_valuation, 0)`)
- ✅ Timestamp casting for consistency
- ✅ Removal of unnecessary columns

**Applied tests**:
- `unique`: `crypto_id` must be unique
- `not_null`: `crypto_id` and `current_price` cannot be null

---

### 🥇 MARTS Layer (Gold Layer)

**Model**: `crypto_market_summary`
- **Type**: TABLE (materialized, optimized for analytics)
- **Schema**: `marts`
- **Purpose**: Analytics-ready data for visualization
- **File**: `dbt/models/marts/crypto_market_summary.sql`

**Calculated metrics**:

1. **Volatility Indicator** (`volatility_24h`)
   - **High**: Price change > 10%
   - **Medium**: Price change between 5-10%
   - **Low**: Price change < 5%

2. **Market Cap Category** (`market_cap_category`)
   - **Large Cap**: Market cap > $10B
   - **Mid Cap**: Market cap $1B - $10B
   - **Small Cap**: Market cap < $1B

3. **Performance Indicator** (`performance_24h`)
   - **Strong Gain**: +5% or more
   - **Gain**: 0% to +5%
   - **Loss**: 0% to -5%
   - **Strong Loss**: -5% or less

4. **Liquidity Ratio** (`volume_to_mcap_ratio`)
   - Daily volume vs market cap ratio
   - Indicates how liquid the asset is
   - Formula: `(total_volume / market_cap) * 100`

**Analytical query examples**:

```sql
-- Distribution of cryptos by market cap category
SELECT
    market_cap_category,
    COUNT(*) as count,
    ROUND(AVG(current_price)::NUMERIC, 2) as avg_price
FROM marts.crypto_market_summary
GROUP BY market_cap_category;

-- Top 10 cryptos with best 24h performance
SELECT
    crypto_name,
    symbol,
    price_change_24h_pct,
    performance_24h
FROM marts.crypto_market_summary
WHERE performance_24h IN ('Strong Gain', 'Gain')
ORDER BY price_change_24h_pct DESC
LIMIT 10;

-- Most liquid cryptos (high relative volume)
SELECT
    crypto_name,
    symbol,
    volume_to_mcap_ratio,
    total_volume
FROM marts.crypto_market_summary
WHERE volume_to_mcap_ratio > 10
ORDER BY volume_to_mcap_ratio DESC;
```

---

### 📊 Data Lineage

```
CoinGecko API
     ↓
raw_crypto_data (Bronze)
     ↓
stg_crypto_data (Silver) ← Quality tests
     ↓
crypto_market_summary (Gold) ← Analytical metrics
     ↓
Dashboard / BI Tool / Analysis
```

---

## Monitoring and Troubleshooting

### View Logs

```bash
# Airflow webserver logs
docker logs de_airflow_webserver

# Airflow scheduler logs
docker logs de_airflow_scheduler

# PostgreSQL logs
docker logs de_postgres

# Follow logs in real-time
docker logs -f de_airflow_scheduler
```

### Common Issues

**Issue**: Airflow UI not loading
```bash
# Restart services
docker-compose restart
```

**Issue**: DAG not appearing
```bash
# Check scheduler logs
docker logs de_airflow_scheduler

# Verify DAG syntax
docker exec -it de_airflow_scheduler airflow dags list
```

**Issue**: dbt connection error
```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Test connection
docker exec -it de_airflow_scheduler psql -h postgres -U airflow -d data_warehouse
```

---

## 🚀 Extensions and Possible Improvements

This project is a perfect template to extend. Here are some ideas:

### 1. **Add more data sources**
```python
# Example: Add crypto news data
def extract_news_data():
    url = "https://newsapi.org/v2/everything?q=cryptocurrency"
    # ... similar to extract_crypto_data
```

### 2. **Implement alerts**
```python
# In Airflow DAG
from airflow.operators.email import EmailOperator

email_alert = EmailOperator(
    task_id='send_alert',
    to='your-email@example.com',
    subject='ETL Pipeline Completed',
    html_content='Pipeline executed successfully'
)
```

### 3. **Add incremental loading in dbt**
```sql
-- In dbt, use incremental materializations
{{ config(materialized='incremental', unique_key='crypto_id') }}
```

### 4. **Connect to a dashboard**
- **Metabase**: Open-source, easy to use
- **Apache Superset**: Powerful and scalable
- **Grafana**: For real-time metrics
- **Power BI / Tableau**: Enterprise solutions

### 5. **Migrate to cloud**
- **AWS**: Airflow → MWAA, PostgreSQL → RDS/Redshift
- **GCP**: Airflow → Composer, PostgreSQL → BigQuery
- **Azure**: Airflow → Data Factory, PostgreSQL → Synapse

---

## 📚 Learning Resources

### Official Documentation
- [Apache Airflow Docs](https://airflow.apache.org/docs/) - Workflow orchestration
- [dbt Documentation](https://docs.getdbt.com/) - SQL transformations
- [CoinGecko API](https://www.coingecko.com/en/api/documentation) - Data source
- [PostgreSQL Docs](https://www.postgresql.org/docs/) - Database

### Recommended Tutorials
- [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
- [dbt Learn](https://courses.getdbt.com/collections) - Free official course
- [Modern Data Stack](https://www.moderndatastack.xyz/) - Ecosystem tools

### Communities
- [dbt Slack Community](https://www.getdbt.com/community/) - 50K+ data practitioners
- [r/dataengineering](https://reddit.com/r/dataengineering) - Reddit community
- [Data Engineering Weekly](https://www.dataengineeringweekly.com/) - Newsletter

---

## 🎯 Use Cases

This project can be adapted for multiple industries:

1. **Finance**: Stock prices, forex rates, financial indicators
2. **E-commerce**: Product prices, inventory levels, sales metrics
3. **Marketing**: Campaign metrics, social media stats, web analytics
4. **IoT**: Sensor data, device telemetry, time series data
5. **Healthcare**: Patient metrics, hospital occupancy, medical device data

---

## 📝 Technical Notes

### Why These Architecture Decisions

**Why Airflow and not just cron jobs?**
- UI for visual monitoring
- Dependency management between tasks
- Automatic retry logic
- Centralized logging
- Scalable to multiple workers

**Why dbt and not just SQL scripts?**
- Version control of transformations
- Automatic data quality tests
- Auto-generated documentation
- Lineage tracking
- Macros and code reusability

**Why PostgreSQL and not a cloud data warehouse?**
- Free and open-source
- Easy to run locally
- Easy to migrate to Redshift, BigQuery, Snowflake
- Good for learning fundamentals

**Why Docker Compose and not Kubernetes?**
- Simpler for local development
- Doesn't require a cluster
- Sufficient for small/medium projects
- Easy to understand for beginners

---

## 📄 License

This project is under the MIT license. You are free to use, modify, and distribute this code.

```
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

---

## ✨ Author

**Portfolio Project - Data Engineering**

Created as a demonstration of skills in:
- 📊 Data Engineering
- 🔄 ETL Pipeline Design
- 🐳 DevOps & Containerization
- 📈 Analytics Engineering
- 🛠️ Modern Data Stack

**Contact**:
- GitHub: [sergiofdezcano](https://github.com/sergiofdezcano)
- LinkedIn: 
- Email: sergiofdezcano@gmail.com

---

## 🙏 Acknowledgments

- CoinGecko for providing a free and reliable API
- Apache Airflow community
- dbt community
- All contributors to the open-source Modern Data Stack

---

## ⭐ Star This Project

If this project was useful to you, consider giving it a star on GitHub!

---

**Questions or issues?** Open an issue on GitHub or contact directly.

**Last updated**: January 2025
