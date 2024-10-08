import pytest 
import pandas as pd
import logging
from src.data_fetcher import NewDataLoader  # Adjust this import if necessary

# Set up logging for the tests
logging.basicConfig(level=logging.INFO)

class TestCryptoDataLoader:

    @pytest.mark.parametrize("filename, expected", [
        ('bitcoin_2010-07-17_2024-10-06.csv', 'bitcoin'),
        ('avalanche_2020-09-22_2024-10-06.csv', 'avalanche'),
        ('binance-coin_2017-08-16_2024-10-06.csv', 'binance-coin'),
    ])
    
    def test_extract_crypto_name_from_filename(self, filename, expected):
        """Test the extract_crypto_name_from_filename function."""
        result = NewDataLoader.extract_crypto_name_from_filename(filename)
        assert result == expected

    def test_rename_and_clean_dataframe(self):
        """Test the rename_and_clean_dataframe function."""
        # Create a DataFrame to test the rename_and_clean_dataframe function
        test_data = {
            'Start': ['2024-10-05', '2024-10-06'],
            'End': ['2024-10-05', '2024-10-06'],
            'Open': [12345.67, 67890.67],
            'High': [12346.57, 67896.65],
            'Low': [12348.45, 67891.67],
            'Close': [12380.50, 67838.56],
            'Volume': [67750456.5, 67750456.5],
            'Market_Cap': [124340235.23, 123440235.23],
        }
        df = pd.DataFrame(test_data)

        # Apply the rename_and_clean_dataframe function
        cleaned_data = NewDataLoader.rename_and_clean_dataframe(df)

        # Assertions
        assert 'Date' in cleaned_data.columns  # Check if 'Start' is renamed to 'Date'
        assert 'End' not in cleaned_data.columns  # Check if 'End' is dropped
        assert 'Start' not in cleaned_data.columns  # Check 'Start' is removed
        assert 'Open' in cleaned_data.columns
        assert 'High' in cleaned_data.columns
        assert 'Low' in cleaned_data.columns
        assert 'Close' in cleaned_data.columns
        assert 'Volume' in cleaned_data.columns
        assert 'Market_Cap' in cleaned_data.columns

        assert cleaned_data.shape[1] == 7  # Should have 7 columns: 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market_Cap'

if __name__ == "__main__":
    pytest.main()