import os
import pandas as pd
import logging
from src.data_loader import DataLoader, DataAggregator
from src.data_fetcher import NewDataLoader, FetchedDataProcessor
from src.database_handler import DatabaseHandler
from src.data_cleaner import PerformCleaning
from src.data_analyzer import PerformCalculations


# Set up logger for the data_loader module
logger = logging.getLogger('data_source_logger')
logger.setLevel(logging.INFO)

log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'
log_file_path = os.path.join(log_directory, 'data_source.log')
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



class MasterData:
    """Class to manage the loading, cleaning, and processing of master data."""
    
    def __init__(self, directory, database_url='C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/cryptocurrency_db.db'):
        self.directory = directory
        self.master_df = None
        self.db_name = database_url
        self.db_handler = DatabaseHandler(f'sqlite:///{database_url}')  

    def load_and_process(self):
        """Load, clean, and process the master data."""
        logger.info("Loading master data...")
        loader = DataLoader(self.directory)
        master_dataframes = loader.load_csv_files()

        if not master_dataframes:
            logger.error("No master data loaded. Exiting the processing.")
            return None

        # Aggregate master data
        self.master_df = DataAggregator.aggregate_data(master_dataframes)

        # Perform cleaning using the PerformCleaning class
        cleaner = PerformCleaning(self.master_df)
        self.master_df = cleaner.clean_all()

        # Ensure the master_df is not empty after cleaning
        if self.master_df is None or self.master_df.empty:
            logger.error("Master DataFrame is empty after cleaning. Exiting the processing.")
            return None

        processor = PerformCalculations(self.master_df)
        self.master_df = processor.calculate_masterdata()        

        logger.info("Master data loaded and processed successfully.")
        return self.master_df


class MasterDataLoader:
    """Class to handle the loading and processing of master data into the database."""
    
    def __init__(self, directory, db_file_path):
        self.directory = directory
        self.db_file_path = db_file_path

        # Initialize logging
        logger.info("Initializing MasterDataLoader...")

        # Initialize database handler
        self.db_handler = DatabaseHandler()

        # Create instance of MasterData
        self.master_data_processor = MasterData(directory)

        # Load and process master data
        self.master_data = self.master_data_processor.load_and_process()
        self.process_master_data()

    def process_master_data(self):
        """Process and save master data to the database."""
        # Check if master_data is valid and create DataFrame
        if self.master_data is not None:
            master_df = pd.DataFrame(self.master_data)
            logger.info("Master data loaded and processed successfully.")
            print("Master Data Sample:")
            print(master_df.head())

            # Save processed data to the database
            try:
                self.db_handler.save_to_database(master_df, table_name='ohlcv_marketcap_data', mode='replace')
                logger.info(f"Processed data saved to database at '{self.db_file_path}' in table 'ohlcv_marketcap_data'.")
            except Exception as e:
                logger.error(f"Error saving to database: {e}")

    def load_master_data(self):
        """Load master data from the database."""
        try:
            master_data = self.db_handler.load_data_from_database(table_name='ohlcv_marketcap_data')
            if master_data is not None:
                logger.info("Master data loaded from the database successfully.")
                return master_data
        except Exception as e:
            logger.error(f"Error loading master data from database: {e}")
            return None


class Fetcher:
    """Class to fetch new data and prepare it for processing."""
    
    def __init__(self):
        self.new_data_df = None
        logger.info("Fetcher initialized.")

    def fetch_and_process_new_data(self):
        """Fetch new data, clean it, and prepare it for aggregation."""
        logger.info("Fetching new data...")

        # Create a NewDataLoader instance to fetch new data
        loader = NewDataLoader()
        fetcher = FetchedDataProcessor(loader)

        # Execute the data fetching and processing workflow
        self.new_data_df = fetcher.execute()

        if self.new_data_df is None or self.new_data_df.empty:
            logger.error("No new data was fetched.")
            return None

        logger.info("New Data Loaded Successfully:")
        print(self.new_data_df)

        # Perform cleaning using PerformCleaning class
        cleaner = PerformCleaning(self.new_data_df)
        self.new_data_df = cleaner.clean_all()

        # Ensure the new_data_df is not empty after cleaning
        if self.new_data_df is None or self.new_data_df.empty:
            logger.error("New DataFrame is empty after cleaning. Exiting the processing.")
            return None

        logger.info("New data cleaned successfully.")


class Aggregator:
    """Class to aggregate master and new data."""
    
    def __init__(self, master_data: pd.DataFrame, new_data_df: pd.DataFrame):
        self.master_data = master_data
        self.new_data_df = new_data_df
        self.aggregated_df = None
        
    def aggregate_data(self):
        """Aggregate master and new data."""
        # Assuming you want to append new data to master data
        aggregated_data = pd.concat([self.master_data, self.new_data_df], ignore_index=True)
        logger.info("Data aggregated successfully.")
        return aggregated_data