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


def extract(spark, data_dir):
    """Read all JSON files from data directory"""
    df = spark.read.option("multiline", "true").json(data_dir)
    return df


def flatten(df):
    """Explode bookings array and flatten nested fields"""
    # capture source file BEFORE explode
    df = df.withColumn("source_file", input_file_name())

    # explode bookings array into rows
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


def split_duplicates(df):
    """Split into clean and duplicate records"""
    window = Window.partitionBy("booking_id").orderBy(desc("loaded_at"))
    df = df.withColumn("row_num", row_number().over(window))

    clean_df = df.filter(col("row_num") == 1).drop("row_num")
    duplicates_df = df.filter(col("row_num") > 1).drop("row_num")

    return clean_df, duplicates_df


def split_invalid(df):
    """Split into valid and invalid records"""
    valid_df = df.filter(col("booking_id").isNotNull())
    invalid_df = df.filter(col("booking_id").isNull())

    return valid_df, invalid_df


def load_to_snowflake(df, table, snowflake_options):
    """Convert Spark DataFrame to pandas and load into Snowflake"""
    import snowflake.connector
    import pandas as pd

    # convert spark df to pandas and replace NaN with None
    import numpy as np
    pandas_df = df.toPandas().replace({np.nan: None})

    if pandas_df.empty:
        print(f"No records to load into {table}")
        return

    conn = snowflake.connector.connect(
        account=snowflake_options['sfURL'].replace('.snowflakecomputing.com', ''),
        user=snowflake_options['sfUser'],
        password=snowflake_options['sfPassword'],
        database=snowflake_options['sfDatabase'],
        warehouse=snowflake_options['sfWarehouse'],
        role=snowflake_options['sfRole']
    )

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


def run(data_dir, snowflake_options):
    spark = create_spark_session()

    # Step 1: Extract
    raw_df = extract(spark, data_dir)

    # Step 2: Flatten
    flat_df = flatten(raw_df)

    # Step 3: Split duplicates
    clean_df, duplicates_df = split_duplicates(flat_df)

    # Step 4: Split invalid
    valid_df, invalid_df = split_invalid(clean_df)

    # Step 5: Load to Snowflake
    load_to_snowflake(valid_df, "raw.bookings", snowflake_options)
    load_to_snowflake(duplicates_df, "raw.bookings_duplicates", snowflake_options)
    load_to_snowflake(invalid_df, "raw.bookings_invalid", snowflake_options)

    print(f"Valid:      {valid_df.count()} records → raw.bookings")
    print(f"Duplicates: {duplicates_df.count()} records → raw.bookings_duplicates")
    print(f"Invalid:    {invalid_df.count()} records → raw.bookings_invalid")


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
