from azure.storage.blob import BlobServiceClient
import json
from azure.eventhub import EventHubConsumerClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
dbutils= WorkspaceClient().dbutils

# Credentials 
event_hub_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials",key="event_hub_connection_string")
event_hub_name = "kaggleeventhub"
blob_storage_connections_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="blob_storage_connection-string")
container_name = "kaggle-pipeline"
storage_account_name = "kaggleglobal"
consumer_group = "$Default"  

# Set up a BlobServiceClient to interact with the Blob storage
blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connections_string)
container_client = blob_service_client.get_container_client(container_name)

# Initialize the EventHubConsumerClient
client = EventHubConsumerClient.from_connection_string(
    event_hub_connection_string,
    consumer_group,
    eventhub_name=event_hub_name
)

#Function to check if the files already laoded not to load the duplicated files 
def file_exists(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    try:
        blob_client.get_blob_properties()
        return True 
    except Exception as e:
        return False 
    
#Function to get the  properties of the event
def on_event(partition_context, event):
    event_data = {
        "event_data": event.body_as_str(encoding="UTF-8"),
        "partition_id": partition_context.partition_id,
        "offset": event.offset,
        "sequence_number": event.sequence_number,
    }

    event_timestamp = event.enqueued_time  
    load_date = event_timestamp.strftime("%Y-%m-%d_%H-%M-%S")  #using the load date to event hub as a unique name

    json_data = json.dumps(event_data) + "\n"
    sequence_number = event.sequence_number
    
    blob_name = f"raw/event_{sequence_number}_{load_date}.json"  

    if file_exists(blob_name):
        print(f"File {blob_name} already exists, skipping upload.")
    else:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data)
        print(f"Uploaded event {event.sequence_number} to blob {blob_name}")

#Function to get events from the hub 
def receive_events():
    try:
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
    receive_events()
