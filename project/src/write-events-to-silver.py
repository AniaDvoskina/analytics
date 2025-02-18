from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import col, from_json

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# Schemas
def define_schemas():
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
    
    return intent_schema, out_schema

# Set up Blob Storage Client
def setup_blob_storage_client():
    blob_storage_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_connection-string")
    container_name = "kaggle-pipeline"  # Define the container name

    # Create Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Set Azure Blob Storage credentials for Spark to access Blob Storage
    spark.conf.set(
        f"fs.azure.account.key.{blob_service_client.account_name}.blob.core.windows.net",
        dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_key")
    )
    
    return blob_service_client, container_name, container_client

# Read data from Delta (Bronze) table
def read_bronze_table(delta_path):
    return spark.read.format("delta").load(delta_path)

# Parse the 'intents' and 'context' columns using the defined schemas above
def parse_data(df, intent_schema, out_schema):
    return df.withColumn("event_data_parsed", from_json(col("intents"), intent_schema)) \
        .withColumn("content_data_parsed", from_json(col("event_data_parsed.context"), out_schema))

# Clean the data by filtering non-null UUID and removing duplicates
def clean_data(df):
    return df.filter(col("uuid").isNotNull()).drop_duplicates(["uuid"])

def write_to_silver(df, container_name, blob_service_client, delta_silver_path):
    df.write.format("delta").mode("overwrite").save(delta_silver_path)

# Main finction
def process_data():
    intent_schema, out_schema = define_schemas()
    
    blob_service_client, container_name, container_client = setup_blob_storage_client()
    
    delta_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/bronze/events"
    
    df = read_bronze_table(delta_path)
    
    df_parsed = parse_data(df, intent_schema, out_schema)
    
    df_cleaned = df_parsed.select(
        col("uuid"),
        col("user_id"),
        col("event_data_parsed.intent").alias("intent"),
        col("event_data_parsed.text").alias("text"),
        col("event_data_parsed.responses").alias("responses"),
        col("event_data_parsed.extension").alias("extension"),
        col("event_data_parsed.context").alias("context"),
        col("event_data_parsed.entityType").alias("entityType"),
        col("event_data_parsed.entities").alias("entities"),
        col("content_data_parsed.out").alias("out")  # Extract 'out' field from 'context'
    )
    
    df_cleaned = clean_data(df_cleaned)
    
    delta_silver_path = f"wasbs://{container_name}@{blob_service_client.account_name}.blob.core.windows.net/silver/events"
    
    # Write the cleaned data to Delta Silver table
    write_to_silver(df_cleaned, container_name, blob_service_client, delta_silver_path)
    print("Data written to Delta successfully.")

# Run the processing function
if __name__ == "__main__":
    process_data()
