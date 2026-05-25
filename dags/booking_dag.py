from airflow.sdk import dag, task
from datetime import datetime

DATA_DIR = '/Users/pujaverma/Desktop/BookingSolution/data'
SNOWFLAKE_CONN_ID = 'snowflake_default'


@dag(
    dag_id='booking_elt_pipeline',
    start_date=datetime(2026, 5, 1),
    schedule='@daily',
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
        """
        PySpark:
        - Extract all JSON files
        - Flatten nested structure
        - Split duplicates → raw.bookings_duplicates
        - Split invalid   → raw.bookings_invalid
        - Load clean data → raw.bookings
        """
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

    setup() >> extract_and_load()


booking_elt_pipeline()
