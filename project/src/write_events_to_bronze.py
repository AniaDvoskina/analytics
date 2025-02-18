from azure.storage.blob import BlobServiceClient
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType
from pyspark.sql.functions import col, from_json

def get_spark_session():
    # Check if we should use a local Spark session (e.g., for unit testing)
    if os.getenv("USE_LOCAL_SPARK", "false").lower() == "true":
        print("Using local Spark session")
        return SparkSession.builder \
            .master("local[*]") \
            .appName("LocalTest") \
            .getOrCreate()
    else:
        # When not in test mode, use Databricks Connect
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()

# Create the Spark session
spark = get_spark_session()

# Set up a dummy dbutils if using local Spark, otherwise use the real one.
if os.getenv("USE_LOCAL_SPARK", "false").lower() == "true":
    print("Using dummy dbutils for local testing")
    # In local mode, you might fetch secrets from environment variables.
    class DummySecrets:
        @staticmethod
        def get(scope, key):
            # For example, return the value from an environment variable (or hard-code dummy values)
            return os.getenv(key, f"dummy_{key}")
    class DummyDBUtils:
        secrets = DummySecrets()
    dbutils = DummyDBUtils()
else:
    # When not in local mode, use the actual dbutils from Databricks
    from databricks.sdk import WorkspaceClient
    dbutils = WorkspaceClient().dbutils

# Azure Blob Storage Configuration: Access credentials and blob container details
blob_storage_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_connection-string")
container_name = "kaggle-pipeline"
raw_folder_path = "raw/"

# Schema definitions as functions
def get_event_data_schema():
    return StructType([
        StructField("intents", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("user_id", StringType(), True)
    ])

def get_main_schema():
    return StructType([
        StructField("event_data", StringType(), True),  # JSON string of event data
        StructField("partition_id", StringType(), True),
        StructField("offset", StringType(), True),
        StructField("sequence_number", StringType(), True),  # Sequence number of the event
        StructField("event_timestamp", TimestampType(), True)
    ])

# Function to initialize the Azure Blob Storage client
def initialize_blob_client(connection_string):
    return BlobServiceClient.from_connection_string(connection_string)

# Function to retrieve a list of JSON files from the Blob Storage container
def get_blob_storage_files(container_client, raw_folder_path, container_name, blob_service_client):
    print("Fetching file list from Blob Storage...")
    # List all blobs in the specified folder path
    blob_list = list(container_client.list_blobs(name_starts_with=raw_folder_path))
    print(f"Total files found: {len(blob_list)}")
    
    # Filter to get only the JSON files
    json_files = [
        f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/{blob.name}"
        for blob in blob_list if blob.name.endswith(".json")
    ]
    return json_files

# Function to load the JSON files into a Spark DataFrame
def load_json_to_spark(json_files, main_schema, event_data_schema):
    if json_files:
        df = spark.read.json(json_files, schema=main_schema)
        
        # Parse the 'event_data' field into its own structure
        df = df.withColumn("event_data", from_json(col("event_data"), event_data_schema))
        
        # Select relevant columns (intents, uuid, user_id, and event_timestamp)
        df = df.select(
            col("event_data.intents"),
            col("event_data.uuid"),
            col("event_data.user_id"),
            col("event_timestamp")
        )
        return df
    else:
        print("No JSON files found in Blob Storage.")
        return None

# Function to write the processed DataFrame to Delta Lake storage
def write_to_delta(df, container_name, blob_service_client, delta_path):
    if df:
        # Construct the path to store the data in Delta format in Blob Storage
        full_delta_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/{delta_path}"
        
        # Write the DataFrame to Delta Lake format
        df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(full_delta_path)
        print("Data written to Delta successfully.")
    else:
        print("No data to write to Delta.")

def clean_data(df):
    return df.filter(col("uuid").isNotNull()).drop_duplicates(["uuid"])

# Main function 
def main():
    # Set up Blob Storage Client using the connection string
    blob_service_client = initialize_blob_client(blob_storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Set Azure Blob Storage credentials for Spark session to access Blob Storage
    spark.conf.set(
        f"fs.azure.account.key.{blob_service_client.account_name}.blob.core.windows.net",
        dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_key")
    )
    
    json_files = get_blob_storage_files(container_client, raw_folder_path, container_name, blob_service_client)
    df = load_json_to_spark(json_files, main_schema=get_main_schema(), event_data_schema=get_event_data_schema())
    df = clean_data(df)
    delta_path = "bronze/events"
    write_to_delta(df, container_name, blob_service_client, delta_path)

if __name__ == "__main__":
    main()
