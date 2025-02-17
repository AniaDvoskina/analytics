from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import col, from_json

# Initialize Databricks and Spark
spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# Define the schema for the nested structure (e.g., event_data)
intent_schema = StructType([
    StructField("intent", StringType(), True),
    StructField("text", ArrayType(StringType()), True),
    StructField("responses", ArrayType(StringType()), True),
    StructField("extension", StringType(), True),
    StructField("context", StringType(), True),
    StructField("entityType", StringType(), True),
    StructField("entities", StringType(), True),
])

out_schema = StructType([
    StructField("out", StringType(), True),
])

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

# Define the Delta path
delta_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/bronze/events"

# Read the Delta table
df = spark.read.format("delta").load(delta_path)

# Parse the 'intents' column and extract the 'intent' field
df_parsed = df.withColumn("event_data_parsed", from_json(col("intents"), intent_schema))\
    .withColumn("content_data_parsed", from_json(col("event_data_parsed.context"), out_schema))

# Select only the necessary fields, including 'out' field
df_parsed = df_parsed.select(
    col("uuid"),
    col("user_id"),
    col("event_data_parsed.intent").alias("intent"),
    col("event_data_parsed.text").alias("text"),
    col("event_data_parsed.responses").alias("responses"),
    col("event_data_parsed.extension").alias("extension"),
    col("event_data_parsed.context").alias("context"),
    col("event_data_parsed.entityType").alias("entityType"),
    col("event_data_parsed.entities").alias("entities"),
    col("content_data_parsed.out").alias("out")  # Extract 'out' field
)

# Keep only rows with non-null uuid
df_cleaned = df_parsed.filter(col("uuid").isNotNull())

# Remove duplicates by 'uuid', assuming that rows with the same 'uuid' are identical
df = df_cleaned.drop_duplicates(["uuid"])

delta_silver_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/silver/events"
df.write.format("delta").mode("overwrite").save(delta_silver_path)