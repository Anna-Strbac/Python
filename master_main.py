import logging
import pandas as pd
import os
from src.data_source import MasterDataLoader, Fetcher, Aggregator
from src.data_analyzer import PerformCalculations
from src.database_handler import DatabaseHandler



def main():
    
    # Specify the directory where you want to store log files
    log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'

    # Create the directory if it doesn't exist
    os.makedirs(log_directory, exist_ok=True)

    # Configure a logger for this module
    logger = logging.getLogger(__name__)  # Use module-level logger
    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    # Specify the log file path
    log_file_path = os.path.join(log_directory, 'master_main.log')

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG to capture all levels of logs
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file_path,  # Use the full path for the log file
        filemode='a'  # Append to the log file
    )
    
    directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/CSV'
    db_file_path = r'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/cryptocurrency_db.db'
    
    # Construct the database URL in the format expected by SQLAlchemy
    database_url = f'sqlite:///{db_file_path}'
    
    logger.info("Starting the data loading process...")
    
    # Create an instance of MasterDataLoader
    data_loader = MasterDataLoader(directory, database_url)

    try:
        # Load master data
        master_data = data_loader.load_master_data()  # Load master data once
        logger.info("Master data loaded successfully.")

        # Convert master data to DataFrame
        master_df = pd.DataFrame(master_data)


        # Save the latest data to the database with mode='replace'
        db_handler = DatabaseHandler()
        db_handler.save_to_database(master_df, table_name='ohlcv_marketcap_data', mode='replace')
        logger.info("Latest data successfully saved to the database.")
        

    except Exception as load_error:
        logger.error(f"Error loading master data: {load_error}")

if __name__ == "__main__":
    main()