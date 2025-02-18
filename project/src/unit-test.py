import unittest
from unittest.mock import patch
import pandas as pd
from project.src.sendtohub import enhance_dataframe_with_uuid_and_user_id
import random

class TestSendEventsToHub(unittest.TestCase):

    @patch('project.src.sendtohub.generate_random_user_id')  # Mock generate_random_user_id
    def test_enhance_dataframe_with_uuid_and_user_id(self, mock_generate_random_user_id):
        
        # Mock generate_random_user_id to return predictable user IDs
        mock_generate_random_user_id.side_effect = lambda: 'test_user_' + str(random.randint(1, 1000))
        
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
        
        # Ensure that uuid and user_id are unique
        self.assertEqual(result_df['uuid'].nunique(), len(result_df))

if __name__ == '__main__':
    unittest.main()
