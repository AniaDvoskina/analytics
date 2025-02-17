from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, BooleanType, MapType
from pyspark.sql.functions import col, from_json

# Initialize Databricks and Spark
spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

#Schemas
event_data_schema = StructType([
    StructField("intents", StringType(), True),  
    StructField("uuid", StringType(), True),
    StructField("user_id", StringType(), True)
])
main_schema = StructType([
    StructField("event_data", StringType(), True),  
    StructField("partition_id", StringType(), True),
    StructField("offset", StringType(), True),
    StructField("sequence_number", StringType(), True)
])

# Azure Blob Storage Configuration
blob_storage_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_connection-string")
container_name = "kaggle-pipeline"
raw_folder_path = "raw/"

# Set up Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Set Azure Blob Storage credentials for Spark
spark.conf.set(
    f"fs.azure.account.key.{blob_service_client.account_name}.blob.core.windows.net",
    dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_key")
)

# List blobs in the container
print("Fetching file list from Blob Storage...")
blob_list = list(container_client.list_blobs(name_starts_with=raw_folder_path))  # Convert iterator to list
print(f"Total files found: {len(blob_list)}")

# Get list of JSON file paths
json_files = [
    f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/{blob.name}" 
    for blob in blob_list if blob.name.endswith(".json")
]
# Load JSON files into Spark DataFrame
if json_files:
    print("Loading files into Spark DataFrame...")
    df = spark.read.json(json_files, schema=main_schema)
    df = df.withColumn("event_data", from_json(col("event_data"), event_data_schema))
    df = df.select(
    col("event_data.intents"),
    col("event_data.uuid"),
    col("event_data.user_id")
)
    #Keeping only rows with the not null uuid 
    df = df.filter(col("uuid").isNotNull())
    #Removing Duplicates by uuid assuming that all the rows with the same uuid are exactly the same 
    df = df.drop_duplicates(["uuid"])

    delta_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/bronze/events"

    df.write.format("delta").mode("overwrite").save(delta_path)

else:
    print("No JSON files found in Blob Storage.")
