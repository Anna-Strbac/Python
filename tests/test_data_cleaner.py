import pandas as pd
import pytest
from src.data_cleaner import DataCleaner

@pytest.fixture
def sample_data():
    """Sample DataFrame for testing."""
    data = {
        'Date': ['2024-10-07', '2024-10-08', '2024-10-08'],
        'Open': [100.9, 200, 150],
        'High': [110, 210, 160.7],
        'Low': [90, 190, 140],
        'Close': [105, 205, 12.6],
        'Volume': [1000, 0, 2000],
        'Market Cap': [500000, -1, 0],
        'CryptocurrencyName': ['bitcoin', 'bitcoin', 'ethereum'],
    }
    return pd.DataFrame(data)


class TestDataCleaner:

    def test_clean_data(self, sample_data):
        """Test the clean_data method."""
        cleaner = DataCleaner(sample_data)
        cleaned_df = cleaner.clean_data()
        
        assert cleaned_df is not None
        assert 'Date' in cleaned_df.columns
        assert cleaned_df.shape[0] == 3  # The original data had 3 rows
        assert cleaned_df['Market Cap'].isna().sum() == 2  # One negative and one zero market cap replaced with NA
        assert cleaned_df['Volume'].isna().sum() == 0  # Expect no NA in Volume due to forward fill
        assert cleaned_df['Volume'].iloc[1] == 1000.0  # Verify that forward fill occurred correctly
    
    def test_remove_duplicates(self):
        """Test the remove_duplicates method."""
        data_with_duplicates = {
            'Date': ['2024-10-07', '2024-10-057', '2024-10-08'],
            'CryptocurrencyName': ['bitcoin', 'bitcoin', 'bitcoin'],
            'Market Cap': [500000, 500000, 200000],
            'Volume': [1000, 1000, 0],
        }
        df = pd.DataFrame(data_with_duplicates)
        cleaner = DataCleaner(df)
        
        cleaner.remove_duplicates()
        assert cleaner.df.shape[0] == 2  # Should have removed one duplicate

    def test_validate_and_clean_data(self, sample_data):
        """Test the validate_and_clean_data method."""
        cleaner = DataCleaner(sample_data)
        volume_na_count, forward_fill_count = cleaner.validate_and_clean_data()
        
        assert volume_na_count == 1  # One row set to NA in 'Volume'
        assert forward_fill_count == 1  # One row forward filled in 'Volume'

    def test_check_and_convert_formats(self, sample_data):
        """Test the check_and_convert_formats method."""
        cleaner = DataCleaner(sample_data)
        
        # Intentionally set wrong data types
        cleaner.df['Volume'] = cleaner.df['Volume'].astype(str)
        
        cleaner.check_and_convert_formats()
        assert pd.api.types.is_numeric_dtype(cleaner.df['Volume'])  # Should be converted to numeric

