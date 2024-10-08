import os
import glob
import pandas as pd
import logging


# Set up logger for the data_loader module
logger = logging.getLogger('data_loader_logger')
logger.setLevel(logging.INFO)

log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'
log_file_path = os.path.join(log_directory, 'data_loader.log')
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class DataLoader:
    """A class to load CSV files from a specified directory."""

    def __init__(self, directory):
        """
        Initialize the DataLoader with the specified directory.

        Args:
            directory (str): The path to the directory containing CSV files.
        """
        self.directory = directory

    def load_csv_files(self):
        """
        Load all CSV files from the specified directory.

        Returns:
            list: A list of DataFrames loaded from CSV files.
        """
        csv_files = glob.glob(os.path.join(self.directory, '*.csv'))
        all_data = []

        for file_path in csv_files:
            df = self._load_and_label_csv(file_path)
            if df is not None:  # Ensure that None DataFrames are not appended
                all_data.append(df)

        if not all_data:
            logger.error("No CSV files found in the directory.")
            return []  # Return empty list if no CSV files found
        
        logger.info(f"Successfully loaded {len(all_data)} CSV files.")
        return all_data

    def _load_and_label_csv(self, file_path):
        """
        Load a CSV file and add a column for the cryptocurrency name.

        Args:
            file_path (str): The path to the CSV file.

        Returns:
            DataFrame: A DataFrame with an added column for the cryptocurrency name.
        """
        try:
            cryptocurrency_name = os.path.splitext(os.path.basename(file_path))[0].split('_')[0]
            df = pd.read_csv(file_path)
            df['CryptocurrencyName'] = cryptocurrency_name
            logger.info(f"Loaded CSV file: {file_path} with cryptocurrency name: {cryptocurrency_name}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {e}")
            return None  # Return None in case of an error


class DataAggregator:
    """A class to aggregate multiple DataFrames into a single DataFrame."""

    @staticmethod
    def aggregate_data(dataframes):
        """
        Aggregate a list of DataFrames into a single DataFrame.

        Args:
            dataframes (list): A list of DataFrames to aggregate.

        Returns:
            DataFrame: A single aggregated DataFrame.
        """
        if not dataframes:
            logger.error("No data to aggregate.")
            return pd.DataFrame()  # Return empty DataFrame if no data

        aggregated_df = pd.concat(dataframes, ignore_index=True)
        aggregated_df.drop(columns=['End'], inplace=True, errors='ignore')  # Ignore if 'End' column does not exist
        aggregated_df.rename(columns={'Start': 'Date'}, inplace=True)

        logger.info(f"Successfully aggregated data into a single DataFrame with {len(aggregated_df)} records.")
        return aggregated_df


