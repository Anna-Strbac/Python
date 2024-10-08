import pandas as pd
import pytest
from src.data_analyzer import DataAnalyzer

@pytest.fixture
def mock_dataframe():
    """Mock dataframe for testing."""
    data = {
        'Date': pd.to_datetime([
            '2024-10-04', '2024-10-05', '2024-10-06', '2024-10-07', '2024-10-08'
        ]),
        'Open': [100, 105, 110, 115, 120],
        'High': [110, 115, 120, 125, 130],
        'Low': [90, 95, 100, 105, 110],
        'Close': [105, 110, 115, 120, 125],
        'Volume': [1000, 2000, 3000, 4000, 5000],
        'CryptocurrencyName': ['bitcoin', 'bitcoin', 'bitcoin', 'ethereum', 'ethereum']
    }
    return pd.DataFrame(data)

def test_calculate_typical_price(mock_dataframe):
    """Test typical price calculation."""
    analyzer = DataAnalyzer(mock_dataframe.copy())
    df_with_typical_price = analyzer.calculate_typical_price()

    print("\nDataFrame after Typical Price Calculation:")
    print(df_with_typical_price[['Open', 'High', 'Low', 'Close', 'Typical_Price']])

    # Assert 'Typical_Price' column is added
    assert 'Typical_Price' in df_with_typical_price.columns

    # Calculate expected typical prices (Open + High + Low + Close) / 4
    expected_typical_prices = (
        mock_dataframe['Open'] + 
        mock_dataframe['High'] + 
        mock_dataframe['Low'] + 
        mock_dataframe['Close']
    ) / 4

    # Print the expected typical prices for comparison
    print("\nExpected Typical Prices:")
    print(expected_typical_prices)

    # Ignore the 'name' attribute while comparing series
    pd.testing.assert_series_equal(
        df_with_typical_price['Typical_Price'], 
        expected_typical_prices, 
        check_names=False
    )


def test_calculate_vwap(mock_dataframe):
    """Test VWAP calculation."""
    analyzer = DataAnalyzer(mock_dataframe.copy())
    analyzer.calculate_typical_price()  # VWAP depends on Typical Price
    df_with_vwap = analyzer.calculate_vwap()

    print("\nDataFrame after VWAP Calculation:")
    print(df_with_vwap[['Typical_Price', 'Volume', 'VWAP']])

    # Assert VWAP column exists
    assert 'VWAP' in df_with_vwap.columns

    # Ensure VWAP column has no NaN values
    assert not df_with_vwap['VWAP'].isna().any()


def test_threshold_determination(mock_dataframe):
    """Test price change threshold determination."""
    analyzer = DataAnalyzer(mock_dataframe.copy())
    thresholds = analyzer.determine_thresholds(percentile=98)

    print("\nThresholds Determined for Price Changes:")
    print(thresholds)

    # Check if thresholds for open and close are calculated
    assert 'Open_Pct_Change' in thresholds
    assert 'Close_Pct_Change' in thresholds

    # Check if thresholds for cryptocurrencies (bitcoin and ethereum) are calculated
    assert 'bitcoin' in thresholds['Open_Pct_Change']
    assert 'ethereum' in thresholds['Open_Pct_Change']


def test_detect_large_changes(mock_dataframe):
    """Test detection of large percentage changes."""
    analyzer = DataAnalyzer(mock_dataframe.copy())
    thresholds = {
        'Open_Pct_Change': {'bitcoin': 5, 'ethereum': 5},
        'High_Pct_Change': {'bitcoin': 5, 'ethereum': 5},
        'Low_Pct_Change': {'bitcoin': 5, 'ethereum': 5},
        'Close_Pct_Change': {'bitcoin': 5, 'ethereum': 5},
        'Volume_Pct_Change': {'bitcoin': 5, 'ethereum': 5},
    }

    # Add some test data for daily percentage changes
    analyzer.df['Open_Daily_Pct_Change'] = [6, 4, 10, 3, 6]
    analyzer.df['High_Daily_Pct_Change'] = [6, 4, 10, 3, 6]
    analyzer.df['Low_Daily_Pct_Change'] = [6, 4, 10, 3, 6]
    analyzer.df['Close_Daily_Pct_Change'] = [6, 4, 10, 3, 6]
    analyzer.df['Volume_Pct_Change'] = [6, 4, 10, 3, 6]

    # Detect large changes based on the thresholds
    large_changes = analyzer.detect_large_changes(thresholds)

    print("\nDetected Large Changes:")
    print(large_changes)

    # Assert that 3 rows have large changes (threshold set at 5%)
    assert len(large_changes) == 3
        
 