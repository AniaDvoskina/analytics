from azure.storage.blob import BlobServiceClient
import json
from pyspark.sql import SparkSession
from azure.eventhub import EventHubConsumerClient
import time
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate()
dbutils= WorkspaceClient().dbutils


# Initialize Spark session
spark = SparkSession.builder.appName("EventHubToBlobStorage").getOrCreate()

# Blob storage connection string and container name
event_hub_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="event_hub_connection_string")
event_hub_name = "product-analytics"
blob_storage_connections_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_connection-string")
container_name = "analytics-pipeline"
storage_account_name = "kaggleanalytics"
storage_account_key = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_key")
delta_table_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/raw/events_delta"
consumer_group = "$Default"  # Consumer group name

# Set up a BlobServiceClient to interact with the Blob storage
blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connections_string)
container_client = blob_service_client.get_container_client(container_name)

# Initialize the EventHubConsumerClient
client = EventHubConsumerClient.from_connection_string(
    event_hub_connection_string,
    consumer_group,
    eventhub_name=event_hub_name
)

def on_event(partition_context, event):
    """Process each event and store it in a Blob."""
    # Prepare the event data to store in the Blob.
    event_data = {
        "event_data": event.body_as_str(encoding="UTF-8"),
        "partition_id": partition_context.partition_id,
        "offset": event.offset,
        "sequence_number": event.sequence_number,
    }

    # Convert the event data to a JSON string
    json_data = json.dumps(event_data) + "\n"
    
    # Create a unique blob name (use sequence number or offset for uniqueness)
    blob_name = f"raw/event_{event.sequence_number}.json"  # Path within the "raw" folder

    # Upload event data to blob storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json_data, overwrite=True)  # Overwrite if blob already exists
    
    print(f"Uploaded event {event.sequence_number} to blob {blob_name}")

def receive_events():
    """Receive events from Event Hub and store them in Blob Storage."""
    try:
        # Receive events from the Event Hub
        print("Receiving events from Event Hub...")
        client.receive(
            on_event=on_event,  # The callback function to process events
            starting_position="-1",  # Start from the latest event
            consumer_group=consumer_group
        )
    except Exception as e:
        print(f"Error while receiving events: {e}")
    finally:
        # Close the client after receiving events
        client.close()

if __name__ == "__main__":
    # Call the function to start receiving events and uploading to Blob Storage
    receive_events()