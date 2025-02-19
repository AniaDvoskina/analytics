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
        # Extra field 'extra_field' is included, but should be ignored by schema
        ('{"intents": "purchase", "uuid": "12345", "user_id": "user1", "extra_field": "should_ignore"}',
         "1", "100", "10", "2024-02-17T12:00:00", "random_data1"),
        ('{"intents": "browse", "uuid": "67890", "user_id": "user2"}',
         "2", "101", "11", "2024-02-17T12:05:00", "random_data2"),
        ('{"uuid": "11111", "user_id": "user3"}',  # Missing "intents"
         "3", "102", "12", "2024-02-17T12:10:00", "random_data3"),
    ]
    
    # Use the main schema 
    return spark.createDataFrame(data, schema=get_main_schema())

def test_schema_validation_with_extra_columns(spark, sample_raw_data_with_extra_columns):
    event_data_schema = get_event_data_schema()

    print("\BEFORE JSON Parsing (Raw Data Schema):")
    sample_raw_data_with_extra_columns.printSchema()
    print("\Raw Data Sample:")
    sample_raw_data_with_extra_columns.show(truncate=False)

    df = sample_raw_data_with_extra_columns.withColumn("event_data", from_json(col("event_data"), event_data_schema))

    print("\AFTER JSON Parsing (Schema Updated):")
    df.printSchema()
    print("\Transformed Data Sample:")
    df.show(truncate=False)
    
    # Expected Schema (ignoring extra fields)
    expected_schema = StructType([
        StructField("intents", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])

    # Assert schema is correct
    assert df.schema == expected_schema, "Schema does not match expected structure"

    # Check for unexpected columns
    unexpected_columns = set(df.columns) - {"intents", "uuid", "user_id", "event_timestamp"}
    assert not unexpected_columns, f"Unexpected columns found: {unexpected_columns}"

    if unexpected_columns:
        print("Unexpected columns found in the DataFrame:")
        print(unexpected_columns)
