import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType
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
    """Simulate raw JSON data from Azure Blob with additional unexpected columns."""
    data = [
        ('{"intents": "purchase", "uuid": "12345", "user_id": "user1", "extra_field": "should_ignore"}',
         "1", "100", "10", "2024-02-17T12:00:00", "random_data1"),
        ('{"intents": "browse", "uuid": "67890", "user_id": "user2", "extra_field": "unexpected_value"}',
         "2", "101", "11", "2024-02-17T12:05:00", "random_data2"),
        ('{"intents": "click", "uuid": "11111", "user_id": "user3"}',
         "3", "102", "12", "2024-02-17T12:10:00", "random_data3"),
    ]
    
    # Extend schema with an unexpected column
    extended_schema = StructType(get_main_schema().fields + [
        StructField("extra_column", StringType(), True)  # Unexpected field
    ])

    return spark.createDataFrame(data, schema=extended_schema)

def test_schema_validation_with_extra_columns(spark, sample_raw_data_with_extra_columns):
    event_data_schema = get_event_data_schema()

    # Parse 'event_data' column with from_json
    df = sample_raw_data_with_extra_columns.withColumn("event_data", from_json(col("event_data"), event_data_schema))

    # Select only expected fields, dropping extra ones
    df = df.select(
        col("event_data.intents"),
        col("event_data.uuid"),
        col("event_data.user_id"),
        col("event_timestamp")
    )

    # Expected Schema (ignoring extra fields)
    expected_schema = StructType([
        StructField("intents", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])

    # Assert that schema matches expectation (ignores extra fields)
    assert df.schema == expected_schema, "Schema does not match expected structure"
    
    # Debugging: Print the DataFrame schema to ensure it's correct
    print("\nTransformed DataFrame Schema:")
    df.printSchema()

    # Check for unexpected columns and print them
    unexpected_columns = set(df.columns) - {"intents", "uuid", "user_id", "event_timestamp"}
    assert not unexpected_columns, f"Unexpected columns found: {unexpected_columns}"

    # Debugging: Print unexpected columns
    if unexpected_columns:
        print("\nUnexpected columns found in the DataFrame:")
        print(unexpected_columns)
