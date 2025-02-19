import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from project.src.write_events_to_bronze import clean_data

# Initialize an in-memory Spark Session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

# Sample test data fixture
@pytest.fixture
def sample_data(spark):
    schema = StructType([
        StructField("uuid", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("intents", StringType(), True)
    ])

    data = [
        ("12345", "user1", "purchase"),
        ("67890", "user2", "browse"),
        ("67890", "user2", "browse"),  # Duplicate
        (None, "user3", "click"),       # Null UUID
    ]
    
    return spark.createDataFrame(data, schema=schema)

#Test for `clean_data`
def test_clean_data(spark, sample_data):
    cleaned_df = clean_data(sample_data)

    print("Original count:", sample_data.count())  
    print("Cleaned count:", cleaned_df.count())    
    print("Null UUIDs count:", cleaned_df.filter(col("uuid").isNull()).count())  
    print("Unique UUIDs count:", cleaned_df.select("uuid").distinct().count())

    # Check that null UUIDs are removed
    assert cleaned_df.filter(col("uuid").isNull()).count() == 0, "Null UUIDs were not removed!"

    # Check that duplicates are removed
    assert cleaned_df.count() == cleaned_df.select("uuid").distinct().count(), "Duplicate UUIDs were not removed!"

