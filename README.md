# BookingSolution — End-to-End ELT Pipeline

A production-grade ELT pipeline built with **PySpark**, **Airflow**, **Snowflake**, **dbt**, and **GitHub Actions CI/CD**.

---

## Architecture

```
JSON Files (dirty data)
      ↓
PySpark (Extract & Load)
  - Reads all JSON files
  - Flattens nested structure
  - Splits duplicates  → raw.bookings_duplicates
  - Splits invalid     → raw.bookings_invalid
  - Loads clean data   → raw.bookings
      ↓
Snowflake (Storage)
  ├── raw.bookings              ← immutable, append only
  ├── raw.bookings_duplicates   ← duplicate records (audit)
  ├── raw.bookings_invalid      ← invalid records (audit)
  ├── staging.stg_bookings      ← cleaned by dbt
  └── marts.marts_bookings      ← business ready
      ↓
dbt (Transform)
  - Type casting
  - Null handling
  - Deduplication
  - Business rules validation
      ↓
GitHub Actions (CI/CD)
  - pytest → dbt compile → dbt test
  - Blocks deployment if tests fail
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Apache Airflow** | Pipeline orchestration |
| **PySpark** | Distributed data processing |
| **Snowflake** | Cloud data warehouse |
| **dbt** | SQL transformations & testing |
| **pytest** | Integration tests |
| **GitHub Actions** | CI/CD pipeline |
| **uv** | Python package management |

---

## Project Structure

```
BookingSolution/
├── .github/
│   └── workflows/
│       └── ci_cd.yml           ← GitHub Actions CI/CD
├── dags/
│   └── booking_dag.py          ← Airflow DAG
├── ingestion/
│   ├── spark_extractor.py      ← PySpark extract + load
│   └── setup_snowflake.py      ← Snowflake table setup
├── booking_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_bookings.sql
│   │   │   └── stg_bookings_duplicates.sql
│   │   └── marts/
│   │       └── marts_bookings.sql
│   ├── macros/
│   │   ├── deduplicate.sql
│   │   └── generate_schema_name.sql
│   └── tests/
│       ├── test_booking_rules.sql
│       └── test_negative_price.sql
├── data/
│   ├── booking_data.json
│   └── booking_data_new.json   ← contains dirty data for testing
└── tests/
    └── test_spark_extractor.py ← pytest integration tests
```

---

## Data Quality Handling

| Issue | Where handled | How |
|---|---|---|
| Duplicate records | PySpark | ROW_NUMBER() dedup before load |
| Null booking_id | PySpark | Split to invalid table |
| Null type/location | dbt staging | COALESCE to 'unknown' |
| Invalid price ('free') | dbt staging | TRY_CAST with COALESCE to 0 |
| Negative price | dbt staging | CASE WHEN < 0 THEN 0 |
| Invalid rating (>5) | dbt test | Custom test warns |
| Wrong case (ATTRACTION) | dbt staging | LOWER() |

---

## Snowflake Layers

```
raw layer      → dirty data, immutable, append only, all VARCHAR
staging layer  → cleaned, type cast, deduplicated (dbt view)
marts layer    → business ready, aggregated (dbt table)
```

---

## Setup

### Prerequisites
- Python 3.11+
- Java (for PySpark)
- Snowflake account
- GitHub account

### Installation

```bash
# Clone the repo
git clone https://github.com/puja-verma11/BookingSolution.git
cd BookingSolution

# Create virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install apache-airflow
uv pip install apache-airflow-providers-snowflake
uv pip install dbt-snowflake
uv pip install pyspark
uv pip install pytest
uv pip install snowflake-connector-python
```

### Environment Variables

```bash
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
```

### Setup Snowflake Tables

```bash
python ingestion/setup_snowflake.py
```

### Run PySpark Extractor

```bash
python ingestion/spark_extractor.py
```

### Run dbt Models

```bash
cd booking_dbt
dbt run
dbt test
```

### Run pytest

```bash
cd ..
pytest tests/test_spark_extractor.py -v
```

### Start Airflow

```bash
airflow standalone
```

---

## CI/CD Pipeline

Every push to `main` triggers:

```
1. Run pytest           → integration tests
2. dbt compile          → SQL syntax check
3. dbt test             → data quality checks
4. Deploy               → only if all pass
```

---

## Highlights

- **ELT not ETL** — raw data loaded first, transformed after
- **Immutable raw layer** — append only with loaded_at metadata
- **PySpark** — handles GBs of data with distributed processing
- **Three Snowflake layers** — raw → staging → marts
- **Quality at every layer** — PySpark validation + dbt tests + pytest
- **CI/CD** — automated testing blocks bad code from production
