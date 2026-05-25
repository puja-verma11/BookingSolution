from airflow.sdk import dag, task
from datetime import datetime
import subprocess
import os

DATA_DIR = '/Users/pujaverma/Desktop/BookingSolution/data'
DBT_DIR = '/Users/pujaverma/Desktop/BookingSolution/booking_dbt'
TESTS_DIR = '/Users/pujaverma/Desktop/BookingSolution'
SNOWFLAKE_CONN_ID = 'snowflake_default'


@dag(
    dag_id='booking_elt_pipeline',
    start_date=datetime(2026, 5, 1),
    schedule='*/30 * * * *',   # every 30 minutes
    catchup=False,
    tags=['booking', 'elt']
)
def booking_elt_pipeline():

    @task()
    def setup():
        """Create Snowflake schemas and tables if not exists"""
        import sys
        sys.path.insert(0, '/Users/pujaverma/Desktop/BookingSolution')
        from ingestion.setup_snowflake import setup_snowflake
        setup_snowflake()

    @task()
    def extract_and_load():
        """PySpark: extract, deduplicate, validate and load to Snowflake raw"""
        import sys
        sys.path.insert(0, '/Users/pujaverma/Desktop/BookingSolution')
        from ingestion.spark_extractor import run
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn_params = hook.get_connection(SNOWFLAKE_CONN_ID)

        snowflake_options = {
            "sfURL": f"{conn_params.extra_dejson.get('account')}.snowflakecomputing.com",
            "sfUser": conn_params.login,
            "sfPassword": conn_params.password,
            "sfDatabase": conn_params.extra_dejson.get('database', 'booking_obj'),
            "sfSchema": "raw",
            "sfWarehouse": conn_params.extra_dejson.get('warehouse', 'COMPUTE_WH'),
            "sfRole": conn_params.extra_dejson.get('role', 'SYSADMIN')
        }

        run(DATA_DIR, snowflake_options)

    @task()
    def dbt_run_staging():
        """Run dbt staging models only"""
        result = subprocess.run(
            ['dbt', 'run', '--select', 'staging'],
            cwd=DBT_DIR,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt staging run failed:\n{result.stderr}")

    @task()
    def dbt_test_staging():
        """Run dbt tests on staging — quality gate"""
        result = subprocess.run(
            ['dbt', 'test', '--select', 'staging'],
            cwd=DBT_DIR,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt staging tests failed:\n{result.stderr}")

    @task()
    def run_pytest():
        """Run pytest integration tests — quality gate"""
        result = subprocess.run(
            ['python', '-m', 'pytest', 'tests/test_spark_extractor.py', '-v'],
            cwd=TESTS_DIR,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"pytest failed:\n{result.stderr}")

    @task()
    def dbt_run_marts():
        """Run dbt marts — only if all quality gates passed"""
        result = subprocess.run(
            ['dbt', 'run', '--select', 'marts'],
            cwd=DBT_DIR,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt marts run failed:\n{result.stderr}")

    # Pipeline flow with quality gates
    setup() >> extract_and_load() >> dbt_run_staging() >> dbt_test_staging() >> run_pytest() >> dbt_run_marts()


booking_elt_pipeline()
