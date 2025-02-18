import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType
from pyspark.sql.functions import col, from_json
from project.src.write_events_to_bronze import get_main_schema, get_event_data_schema

# Initialize an in-memory Spark Session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

# Sample test data (simulating raw JSON data from Azure Blob)
@pytest.fixture
def sample_raw_data(spark):
    data = [
        ('{"intents": "purchase", "uuid": "12345", "user_id": "user1"}', "1", "100", "10", "2024-02-17T12:00:00"),
        ('{"intents": "browse", "uuid": "67890", "user_id": "user2"}', "2", "101", "11", "2024-02-17T12:05:00")
    ]
    
    main_schema = get_main_schema()  # Use the schema function
    return spark.createDataFrame(data, schema=main_schema)

# Schema Validation Test
def test_schema_validation(spark, sample_raw_data):
    event_data_schema = get_event_data_schema()  # Use the event schema function
    df = sample_raw_data.withColumn("event_data", from_json(col("event_data"), event_data_schema))
    
    # Extract nested fields
    df = df.select(
        col("event_data.intents"),
        col("event_data.uuid"),
        col("event_data.user_id"),
        col("event_timestamp")
    )

    # Validate schema
    expected_schema = StructType([
        StructField("intents", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])

    assert df.schema == expected_schema, "Schema does not match expected structure"
