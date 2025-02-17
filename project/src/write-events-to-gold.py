from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import col, from_json

# Initialize Databricks and Spark
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

storage_folder = f"abfss://unity-catalog-storage@dbstorageewb5nfkelzgwu.dfs.core.windows.net/1803686465879368"
schema = "gold"
table_name = "events"
storage_path = f"{storage_folder}/{schema}/{table_name}"
unity_table_path=f"kaggle.{schema}.{table_name}"

def write_table(df, unity_table_path: str, storage_path: str):
    writeDf = df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("path", storage_path)

    writeDf.saveAsTable(unity_table_path)

# Define the Delta path
delta_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/silver/events"

# Read the Delta table
df = spark.read.format("delta").load(delta_path)
df = df.select("uuid", "user_id", "intent", "text", "responses","out")  
write_table(df,unity_table_path, storage_path)


