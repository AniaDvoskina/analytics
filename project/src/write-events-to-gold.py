from pyspark.sql.functions import col, current_timestamp, from_json
from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# Azure Blob Storage Configuration
blob_storage_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_connection-string")
container_name = "kaggle-pipeline"

# Set up Blob Storage Client
blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Set Azure Blob Storage credentials for Spark
spark.conf.set(
    f"fs.azure.account.key.{blob_service_client.account_name}.blob.core.windows.net",
    dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_key")
)

# Azure Storage Path Configuration
storage_folder = f"abfss://unity-catalog-storage@dbstorageewb5nfkelzgwu.dfs.core.windows.net/1803686465879368"
schema = "gold"
table_name = "events"
storage_path = f"{storage_folder}/{schema}/{table_name}"
unity_table_path = f"kaggle.{schema}.{table_name}"

# Function to initialize the Blob Service Client
def initialize_blob_client(blob_storage_connection_string):
    return BlobServiceClient.from_connection_string(blob_storage_connection_string)

# Function to set Spark Azure Blob Storage credentials
def set_spark_blob_storage_credentials(blob_service_client, dbutils):
    spark.conf.set(
        f"fs.azure.account.key.{blob_service_client.account_name}.blob.core.windows.net",
        dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_key")
    )

def read_delta_table(delta_path):
    df = spark.read.format("delta").load(delta_path)
    df = df.withColumn("loaddate", current_timestamp())
    return df 

# Function to write DataFrame to Unity Catalog as a Delta table
def write_table(df, unity_table_path: str, storage_path: str):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("path", storage_path) \
        .saveAsTable(unity_table_path)


blob_service_client = initialize_blob_client(blob_storage_connection_string)

set_spark_blob_storage_credentials(blob_service_client, dbutils)

delta_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/silver/events"

df = read_delta_table(delta_path)

df = df.select("uuid", "user_id", "intent", "text", "responses", "out", "loaddate")

# Write the processed DataFrame to Unity catalog as Delta table
write_table(df, unity_table_path, storage_path)
