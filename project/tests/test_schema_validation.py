import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, from_json
from project.src.write_events_to_bronze import get_main_schema, get_event_data_schema

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

@pytest.fixture
def sample_raw_data_with_extra_columns(spark):
    """Simulate raw JSON data with extra and missing fields."""
    data = [
        ('{"intents": "purchase", "uuid": "12345", "user_id": "user1"}',
         "1", "100", "10", "2024-02-17T12:00:00", "random_data1"),
        ('{"intents": "browse", "uuid": "67890", "user_id": "user2"}',
         "2", "101", "11", "2024-02-17T12:05:00", "random_data2"),
        ('{"uuid": "11111", "user_id": "user3"}',  # Missing "intents"
         "3", "102", "12", "2024-02-17T12:10:00", "random_data3"),
    ]
    return spark.createDataFrame(data, schema=get_main_schema())

def test_schema_validation_with_missing_fields(spark, sample_raw_data_with_extra_columns):
    # Define the event data schema (this is the schema for the 'event_data' field)
    event_data_schema = get_event_data_schema()

    # Parse the 'event_data' field into its structured format using the defined schema
    df = sample_raw_data_with_extra_columns.withColumn("event_data", from_json(col("event_data"), event_data_schema))

    # Select only the relevant fields and ignore the extra fields
    df = df.select(
        col("event_data.intents"),
        col("event_data.uuid"),
        col("event_data.user_id"),
        col("event_timestamp")
    )

    # Print the final columns after selection
    print("\nFINAL Selected Columns:")
    print(df.columns)

    # Check for missing 'intents' field and ensure it is null where expected
    missing_intents_df = df.filter(col("intents").isNull())
    assert missing_intents_df.count() > 0, "Expected rows with missing 'intents' field"

    # Ensure the schema matches the expected one
    expected_columns = {"intents", "uuid", "user_id", "event_timestamp"}
    assert set(df.columns) == expected_columns, f"Unexpected columns found: {df.columns}"

    # Verify that extra fields are not included in the DataFrame (i.e., extra_field is ignored)
    extra_fields = set(df.columns) - expected_columns
    assert not extra_fields, f"Unexpected extra fields found: {extra_fields}"

    print("Test Passed! No extra fields and missing fields are handled correctly.")
