import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import random
import string
import uuid
import json
from project.src.sendtohub import enhance_dataframe_with_uuid_and_user_id, fetch_json_from_kaggle, send_data_to_eventhub

class TestSendEventsToHub(unittest.TestCase):

    @patch('project.src.sendtohub.DatabricksSession')
    @patch('project.src.sendtohub.dbutils.secrets.get')
    @patch('project.src.sendtohub.requests.get')
    @patch('project.src.sendtohub.generate_random_user_id')
    @patch('project.src.sendtohub.EventHubProducerClient')
    def test_enhance_dataframe_with_uuid_and_user_id(self, MockEventHubProducerClient, mock_generate_random_user_id, mock_requests_get, mock_dbutils_get, MockDatabricksSession):
        
        # Mock the DatabricksSession
        mock_databricks_instance = MagicMock()
        MockDatabricksSession.builder.getOrCreate.return_value = mock_databricks_instance
        
        # Mock the secrets.get method to return test credentials
        mock_dbutils_get.side_effect = lambda scope, key: "mocked_value" if key != "kaggle_password" else "mocked_password"
        
        # Mock the requests.get method to return a successful response with mocked JSON data
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        
        # Mock generate_random_user_id to return predictable user IDs
        mock_generate_random_user_id.side_effect = lambda: 'test_user_' + str(random.randint(1, 1000))
        
        # Mock the EventHubProducerClient
        mock_eventhub_producer_instance = MagicMock()
        MockEventHubProducerClient.from_connection_string.return_value = mock_eventhub_producer_instance
        
        # Create a sample dataframe
        data = {"col1": [1, 2, 3], "col2": ['a', 'b', 'c']}
        df = pd.DataFrame(data)
        
        # Enhance DataFrame with UUID and user ID
        result_df = enhance_dataframe_with_uuid_and_user_id(df)
        
        # Check that 'uuid' and 'user_id' columns are added
        self.assertIn('uuid', result_df.columns)
        self.assertIn('user_id', result_df.columns)
        
        # Check that user_id is now different for each row
        self.assertEqual(result_df['user_id'].nunique(), len(result_df))  # This will pass now
        
        # Check that uuid is a valid string (non-null)
        self.assertTrue(result_df['uuid'].apply(lambda x: isinstance(x, str)).all())
        
        # Check that uuid and user_id are unique
        self.assertEqual(result_df['uuid'].nunique(), len(result_df))

        # Check that the EventHubProducerClient was called
        MockEventHubProducerClient.from_connection_string.assert_called_once_with("mocked_value", eventhub_name="kaggleeventhub")
        
        # Ensure that the correct data would be sent to Event Hub (check that a batch was created)
        mock_eventhub_producer_instance.create_batch.assert_called()
        
        # Ensure dbutils.secrets.get was called to retrieve event hub connection string and kaggle password
        mock_dbutils_get.assert_any_call(scope="kaggle-project-credentials", key="event_hub_connection_string")
        mock_dbutils_get.assert_any_call(scope="kaggle-project-credentials", key="kaggle_password")
        
        # Ensure requests.get was called to fetch data from Kaggle
        mock_requests_get.assert_called_with('https://www.kaggle.com/api/v1/datasets/download/tusharpaul2001/university-chatbot-dataset/intents.json', auth=('annadvoskina', 'mocked_password'))
        
        # Ensure that Databricks session was not called during the test
        MockDatabricksSession.builder.getOrCreate.assert_not_called()

if __name__ == '__main__':
    unittest.main()
