import pandas as pd
from src.data_source import Aggregator


class TestAggregator:

    def test_aggregate_data(self):
        """Test the aggregate_data method."""
        master = pd.DataFrame({
            'Date': ['2024-10-07'],
            'Open': [100.9]
        })
        new = pd.DataFrame({
            'Date': ['2024-10-08'],
            'Open': [200.0]
        })

        aggregator = Aggregator(master, new)
        aggregated_data = aggregator.aggregate_data()

        assert aggregated_data.shape[0] == 2  # Ensure the new row was appended
        assert '2024-10-08' in aggregated_data['Date'].values