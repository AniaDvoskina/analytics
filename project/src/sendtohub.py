import json
import pandas as pd
import asyncio
import uuid
import random
import string
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from requests.auth import HTTPBasicAuth
import requests

spark = DatabricksSession.builder.getOrCreate()
dbutils = WorkspaceClient().dbutils

# Retrieve credentials
event_hub_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials", key="event_hub_connection_string")
kaggle_password = dbutils.secrets.get(scope="kaggle-project-credentials", key="kaggle_password")
event_hub_name = "kaggleeventhub"
kaggle_username = "annadvoskina"

# Define Kaggle API endpoint
kaggle_endpoint = 'https://www.kaggle.com/api/v1/datasets/download/tusharpaul2001/university-chatbot-dataset/intents.json'
login = HTTPBasicAuth(kaggle_username, kaggle_password)

# Function to retrieve JSON data from Kaggle
def fetch_json_from_kaggle(endpoint: str, auth: HTTPBasicAuth = None) -> dict:
    response = requests.get(endpoint, auth=auth)
    if response.status_code == 200:
        try:
            return response.json()
        except (ValueError, requests.exceptions.JSONDecodeError) as e:
            raise ValueError(f"Error decoding JSON response: {e}")
    else:
        response.raise_for_status()

# Function to generate a random user ID
def generate_random_user_id(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Function to enhance dataframe with UUID and user ID
def enhance_dataframe_with_uuid_and_user_id(df: pd.DataFrame) -> pd.DataFrame:
    df['uuid'] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    df['user_id'] = df.apply(lambda _: generate_random_user_id(), axis=1)
    return df

# Function to send data to Event Hub
async def send_data_to_eventhub(df: pd.DataFrame):
    producer = EventHubProducerClient.from_connection_string(event_hub_connection_string, eventhub_name=event_hub_name)
    async with producer:
        for _, row in df.iterrows():
            event_data_batch = await producer.create_batch()
            event_json = json.dumps(row.to_dict())
            event_data_batch.add(EventData(event_json))
            await producer.send_batch(event_data_batch)
    print('Data successfully sent to Event Hub.')

# Main logic to fetch data, enhance it, and send it to Event Hub
def main():
    # Fetch data from Kaggle
    json_data = fetch_json_from_kaggle(kaggle_endpoint)
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(json_data)
    
    # Enhance DataFrame with UUID and user ID
    df = enhance_dataframe_with_uuid_and_user_id(df)
    
    # Send data to Event Hub
    asyncio.run(send_data_to_eventhub(df))

if __name__ == '__main__':
    main()
