import json
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubConsumerClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# Retrieve credentials
event_hub_connection_string = dbutils.secrets.get(scope = "kaggle-project-credentials", key= "event_hub_connection_string")
event_hub_name = "kaggleeventhub"
blob_storage_connection_string = dbutils.secrets.get(scope = "kaggle-project-credentials", key= "blob_storage_connection-string")
container_name = "kaggle-pipeline"
consumer_group = "$Default"

# Initialize Azure Blob Storage Client
def initialize_blob_storage(blob_storage_connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    return blob_service_client, container_client

# Initialize Event Hub Consumer Client
def initialize_eventhub_client(event_hub_connection_string, event_hub_name, consumer_group):
    client = EventHubConsumerClient.from_connection_string(
        event_hub_connection_string,
        consumer_group,
        eventhub_name=event_hub_name
    )
    return client

# Check if file already exists in Blob Storage
def file_exists(blob_name, container_client):
    blob_client = container_client.get_blob_client(blob_name)
    try:
        blob_client.get_blob_properties()
        return True  
    except Exception as e:
        return False  

# Event handler function for processing events from Event Hub
def on_event(partition_context, event, container_client):
    event_data = {
        "event_data": event.body_as_str(encoding="UTF-8"),
        "partition_id": partition_context.partition_id,
        "offset": event.offset,
        "sequence_number": event.sequence_number,
    }

    event_timestamp = event.enqueued_time
    load_date = event_timestamp.strftime("%Y-%m-%d_%H-%M-%S") 

    json_data = json.dumps(event_data) + "\n"
    sequence_number = event.sequence_number
    
    # Generate the blob name using sequence number and load date
    blob_name = f"raw/event_{sequence_number}_{load_date}.json"

    # Check if file already exists in Blob Storage
    if file_exists(blob_name, container_client):
        print(f"File {blob_name} already exists, skipping upload.")
    else:
        # Upload the event data to Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data)
        print(f"Uploaded event {event.sequence_number} to blob {blob_name}")

# Function to receive events from Event Hub
def receive_events(client, consumer_group, container_client):
    try:
        print("Receiving events from Event Hub...")
        client.receive(
            on_event=lambda partition_context, event: on_event(partition_context, event, container_client),
            starting_position="-1",  # Start from the latest event
            consumer_group=consumer_group
        )
    except Exception as e:
        print(f"Error while receiving events: {e}")
    finally:
        # Close the client after receiving events
        client.close()

# Main function to run the process
def main():  
    # Initialize Blob Storage and Event Hub clients
    blob_service_client, container_client = initialize_blob_storage(blob_storage_connection_string, container_name)
    client = initialize_eventhub_client(event_hub_connection_string, event_hub_name, consumer_group)

    # Start receiving events from Event Hub
    receive_events(client, consumer_group, container_client)

if __name__ == "__main__":
    main()
