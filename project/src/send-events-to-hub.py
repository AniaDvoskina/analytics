from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json 
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
from requests.auth import HTTPBasicAuth
import requests
import asyncio 
import random
import string 
import uuid

spark = DatabricksSession.builder.getOrCreate()
dbutils= WorkspaceClient().dbutils

#get secrets 
event_hub_connection_string = dbutils.secrets.get(scope="kaggle-project-credentials",key="event_hub_connection_string")
event_hub_name = "kaggleeventhub"
kaggle_username = "annadvoskina"
kaggle_password = dbutils.secrets.get(scope="kaggle-project-credentials",key="kaggle_password")

#kaggle
endpoint = 'https://www.kaggle.com/api/v1/datasets/download/tusharpaul2001/university-chatbot-dataset/intents.json'
login = HTTPBasicAuth(kaggle_username,kaggle_password)

def get_json_from_kaggle(endpoint, auth=None):
    response = requests.get(endpoint, auth=auth)
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except (ValueError, requests.exceptions.JSONDecodeError) as e:
            raise ValueError(f"Error decoding JSON response: {e}")
    else:
        response.raise_for_status()  

async def send_to_eventhub(df):
    producer = EventHubProducerClient.from_connection_string(event_hub_connection_string, eventhub_name=event_hub_name)
    async with producer:
        for index, row in df.iterrows():
            event_data_batch = await producer.create_batch()
            row_data = row.to_dict()
            event_json = json.dumps(row_data)  
            
            event_data_batch.add(EventData(event_json))
            
            await producer.send_batch(event_data_batch)
    print('Loaded in event hub successfully')

import random
import string

# Function to generate a random user ID
def generate_random_user_id():
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    return random_string 

def enhance_with_uuid_and_user_id(df):
    df['uuid'] = df.apply(lambda _: str(uuid.uuid4()), axis=1)  # Adding a UUID to each row
    df['user_id'] = df.apply(lambda _: generate_random_user_id(), axis=1)  # Adding a random user ID (more random)
    return df

json_data = get_json_from_kaggle(endpoint)
df = pd.DataFrame(json_data) 
df = enhance_with_uuid_and_user_id(df)
asyncio.run(send_to_eventhub(df))   