from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    row_number, desc, col, explode,
    current_timestamp, input_file_name
)
from pyspark.sql.types import StringType


def create_spark_session():
    return SparkSession.builder \
        .appName("BookingIngestion") \
        .master("local") \
        .getOrCreate()


def extract(spark, file_path):
    """Read a single file — detects type from extension"""
    if file_path.endswith('.json'):
        return spark.read.option("multiline", "true").json(file_path)
    elif file_path.endswith('.parquet'):
        return spark.read.parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


def flatten(df):
    df = df.withColumn("source_file", input_file_name())
    df = df.select(
        explode(col("bookings")).alias("booking"),
        col("source_file")
    )
    return df.select(
        col("booking.booking_id").cast(StringType()).alias("booking_id"),
        col("booking.type").cast(StringType()).alias("type"),
        col("booking.location").cast(StringType()).alias("location"),
        col("booking.metadata.name").alias("name"),
        col("booking.metadata.category").alias("category"),
        col("booking.metadata.rating").cast(StringType()).alias("rating"),
        col("booking.metadata.price").cast(StringType()).alias("price"),
        current_timestamp().alias("loaded_at"),
        col("source_file")
    )


def flatten_parquet(df):
    """Parquet is already flat — just add metadata columns"""
    return df.select(
        col("booking_id").cast(StringType()).alias("booking_id"),
        col("type").cast(StringType()).alias("type"),
        col("location").cast(StringType()).alias("location"),
        col("name").cast(StringType()).alias("name"),
        col("category").cast(StringType()).alias("category"),
        col("rating").cast(StringType()).alias("rating"),
        col("price").cast(StringType()).alias("price"),
        current_timestamp().alias("loaded_at"),
        input_file_name().alias("source_file")
    )


def split_duplicates(df):
    window = Window.partitionBy("booking_id").orderBy(desc("loaded_at"))
    df = df.withColumn("row_num", row_number().over(window))
    clean_df = df.filter(col("row_num") == 1).drop("row_num")
    duplicates_df = df.filter(col("row_num") > 1).drop("row_num")
    return clean_df, duplicates_df


def split_invalid(df):
    valid_df = df.filter(col("booking_id").isNotNull())
    invalid_df = df.filter(col("booking_id").isNull())
    return valid_df, invalid_df


# ── Snowflake helpers ─────────────────────────────────────────────

def get_snowflake_connection(snowflake_options):
    """Single place to create a Snowflake connection"""
    import snowflake.connector
    return snowflake.connector.connect(
        account=snowflake_options['sfURL'].replace('.snowflakecomputing.com', ''),
        user=snowflake_options['sfUser'],
        password=snowflake_options['sfPassword'],
        database=snowflake_options['sfDatabase'],
        warehouse=snowflake_options['sfWarehouse'],
        role=snowflake_options['sfRole']
    )


def get_processed_files(snowflake_options):
    """Return set of filenames already loaded"""
    conn = get_snowflake_connection(snowflake_options)
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM booking_obj.raw.processed_files")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return set(row[0] for row in rows)


def mark_file_processed(filename, snowflake_options):
    """Record filename as successfully processed"""
    conn = get_snowflake_connection(snowflake_options)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO booking_obj.raw.processed_files (filename) VALUES (%s)",
        (filename,)
    )
    cursor.close()
    conn.close()
    print(f"Marked as processed: {filename}")


def load_to_snowflake(df, table, snowflake_options):
    """Convert Spark DataFrame to pandas and load into Snowflake"""
    import pandas as pd
    import numpy as np

    pandas_df = df.toPandas().replace({np.nan: None})

    if pandas_df.empty:
        print(f"No records to load into {table}")
        return

    conn = get_snowflake_connection(snowflake_options)   # ← uses helper now
    cursor = conn.cursor()
    schema, table_name = table.split('.')

    for _, row in pandas_df.iterrows():
        cursor.execute(f"""
            INSERT INTO booking_obj.{schema}.{table_name}
            (booking_id, type, location, name, category, rating, price, loaded_at, source_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
        """, (
            row['booking_id'], row['type'], row['location'],
            row['name'], row['category'], row['rating'],
            row['price'], row['source_file']
        ))

    cursor.close()
    conn.close()
    print(f"Loaded {len(pandas_df)} records into {table}")


# ── Main pipeline ─────────────────────────────────────────────────

def run(data_dir, snowflake_options):
    spark = create_spark_session()

    # Check which files already processed
    processed = get_processed_files(snowflake_options)
    print(f"Already processed: {processed}")

    # Only pick new files — both JSON and parquet
    import glob
    all_files = (
        glob.glob(f"{data_dir}/*.json") +
        glob.glob(f"{data_dir}/*.parquet")
    )
    new_files = [f for f in all_files if f not in processed]

    if not new_files:
        print("No new files to process. Skipping.")
        return

    print(f"New files to process: {new_files}")

    # Process each file individually (different types need different flatten)
    for file_path in new_files:
        print(f"\nProcessing: {file_path}")

        raw_df = extract(spark, file_path)

        # Choose flatten based on file type
        if file_path.endswith('.json'):
            flat_df = flatten(raw_df)
        elif file_path.endswith('.parquet'):
            flat_df = flatten_parquet(raw_df)

        clean_df, duplicates_df = split_duplicates(flat_df)
        valid_df, invalid_df = split_invalid(clean_df)

        load_to_snowflake(valid_df, "raw.bookings", snowflake_options)
        load_to_snowflake(duplicates_df, "raw.bookings_duplicates", snowflake_options)
        load_to_snowflake(invalid_df, "raw.bookings_invalid", snowflake_options)

        print(f"Valid:      {valid_df.count()} records → raw.bookings")
        print(f"Duplicates: {duplicates_df.count()} records → raw.bookings_duplicates")
        print(f"Invalid:    {invalid_df.count()} records → raw.bookings_invalid")

        mark_file_processed(file_path, snowflake_options)


if __name__ == "__main__":
    DATA_DIR = "/Users/pujaverma/Desktop/BookingSolution/data"
    import os
    SNOWFLAKE_OPTIONS = {
        "sfURL": "udihzux-sgc13505.snowflakecomputing.com",
        "sfUser": os.environ.get("SNOWFLAKE_USER"),
        "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
        "sfDatabase": "booking_obj",
        "sfSchema": "raw",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "SYSADMIN"
    }
    run(DATA_DIR, SNOWFLAKE_OPTIONS)