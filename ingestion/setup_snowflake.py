import snowflake.connector
import os


def setup_snowflake():
    conn = snowflake.connector.connect(
        account='UDIHZUX-SGC13505',
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        database='booking_obj',
        warehouse='COMPUTE_WH',
        role='SYSADMIN'
    )

    cursor = conn.cursor()

    statements = [
        # raw tables
        """CREATE TABLE IF NOT EXISTS booking_obj.raw.bookings (
            booking_id   VARCHAR,
            type         VARCHAR,
            location     VARCHAR,
            name         VARCHAR,
            category     VARCHAR,
            rating       VARCHAR,
            price        VARCHAR,
            loaded_at    TIMESTAMP,
            source_file  VARCHAR
        )""",
        """CREATE TABLE IF NOT EXISTS booking_obj.raw.bookings_duplicates (
            booking_id   VARCHAR,
            type         VARCHAR,
            location     VARCHAR,
            name         VARCHAR,
            category     VARCHAR,
            rating       VARCHAR,
            price        VARCHAR,
            loaded_at    TIMESTAMP,
            source_file  VARCHAR
        )""",
        """CREATE TABLE IF NOT EXISTS booking_obj.raw.bookings_invalid (
            booking_id   VARCHAR,
            type         VARCHAR,
            location     VARCHAR,
            name         VARCHAR,
            category     VARCHAR,
            rating       VARCHAR,
            price        VARCHAR,
            loaded_at    TIMESTAMP,
            source_file  VARCHAR
        )""",

        # schemas
        "CREATE SCHEMA IF NOT EXISTS booking_obj.staging",
        "CREATE SCHEMA IF NOT EXISTS booking_obj.marts",
    ]

    for stmt in statements:
        cursor.execute(stmt)
        print(f"OK: {stmt.strip()[:60]}...")

    cursor.close()
    conn.close()
    print("\nSnowflake setup complete!")


if __name__ == "__main__":
    setup_snowflake()
