import pytest
import snowflake.connector
import os


@pytest.fixture(scope="session")
def snowflake_conn():
    """Connect to Snowflake"""
    conn = snowflake.connector.connect(
        account='UDIHZUX-SGC13505',
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        database='booking_obj',
        warehouse='COMPUTE_WH',
        role='SYSADMIN'
    )
    yield conn
    conn.close()


class TestStagingTable:

    def test_no_duplicates(self, snowflake_conn):
        """staging table should have no duplicate booking_ids"""
        cursor = snowflake_conn.cursor()
        cursor.execute("""
            SELECT booking_id, COUNT(*) as cnt
            FROM booking_obj.staging.stg_bookings
            GROUP BY booking_id
            HAVING cnt > 1
        """)
        duplicates = cursor.fetchall()
        assert len(duplicates) == 0, f"Found duplicates: {duplicates}"

    def test_no_null_booking_id(self, snowflake_conn):
        """staging table should have no null booking_ids"""
        cursor = snowflake_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM booking_obj.staging.stg_bookings
            WHERE booking_id IS NULL
        """)
        count = cursor.fetchone()[0]
        assert count == 0, f"Found {count} null booking_ids"

    def test_no_null_location(self, snowflake_conn):
        """staging table should have no null locations"""
        cursor = snowflake_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM booking_obj.staging.stg_bookings
            WHERE location IS NULL
        """)
        count = cursor.fetchone()[0]
        assert count == 0, f"Found {count} null locations"

    def test_valid_type_values(self, snowflake_conn):
        """type column should only contain known values"""
        cursor = snowflake_conn.cursor()
        cursor.execute("""
            SELECT DISTINCT type
            FROM booking_obj.staging.stg_bookings
            WHERE type NOT IN ('attraction', 'hotel', 'restaurant', 'flight', 'unknown')        """)
        invalid_types = cursor.fetchall()
        assert len(invalid_types) == 0, f"Found invalid types: {invalid_types}"

    def test_price_not_negative(self, snowflake_conn):
        """price should never be negative in staging"""
        cursor = snowflake_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM booking_obj.staging.stg_bookings
            WHERE price < 0
        """)
        count = cursor.fetchone()[0]
        assert count == 0, f"Found {count} negative prices"

    def test_staging_has_data(self, snowflake_conn):
        """staging table should not be empty"""
        cursor = snowflake_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM booking_obj.staging.stg_bookings")
        count = cursor.fetchone()[0]
        assert count > 0, "Staging table is empty!"
