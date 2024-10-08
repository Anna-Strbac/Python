import logging
import pandas as pd
import os

from src.data_source import MasterDataLoader, Fetcher, Aggregator
from src.data_analyzer import PerformCalculations
from src.database_handler import DatabaseHandler
from src.data_cleaner import DataFormatter


def main():
    # Specify the directory where you want to store log files
    log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'

    # Create the directory if it doesn't exist
    os.makedirs(log_directory, exist_ok=True)

    # Configure a logger for this module
    logger = logging.getLogger(__name__)  # Use module-level logger
    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    # Specify the log file path
    log_file_path = os.path.join(log_directory, 'main.log')

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG to capture all levels of logs
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file_path,  # Use the full path for the log file
        filemode='a'  # Append to the log file
    )

    logger.info("Application started.")

    db_file_path = r'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/cryptocurrency_db.db'
    
    # Construct the database URL in the format expected by SQLAlchemy
    database_url = f'sqlite:///{db_file_path}'
    logger.debug(f"Database URL constructed: {database_url}")

    # Create an instance of DatabaseHandler
    try:
        db_handler = DatabaseHandler()
        logger.info("DatabaseHandler instance created.")
    except Exception as e:
        logger.error(f"Failed to create DatabaseHandler instance: {e}")
        return

    # Step 1: Load the previous day's data from the database
    try:
        # Check if the database file exists
        if not os.path.exists(db_handler.database_url):
            logger.error(f"Database file not found: {db_handler.database_url}")
            return
        else:
            logger.debug(f"Database file found: {db_handler.database_url}")

        query = "SELECT * FROM ohlcv_marketcap_data"  # Load the existing data from the database
        previous_data = pd.read_sql_query(query, db_handler.engine)
        logger.info("Successfully loaded previous day's data from the database.")
        logger.debug(f"Previous data shape: {previous_data.shape}")

        # Display the loaded previous data
        print("\nPrevious Data from the Database:")
        print(previous_data.head(30))  # Display the first 30 rows of the previous data

        logger.debug("Displayed head of previous_data.")
        
    except Exception as e:
        logger.error(f"Error loading data from the database: {e}")
        return


    # Create Fetcher instance
    fetcher = Fetcher()  # Pass master_data instance if needed

    # Fetch and process new data
    try:
        fetcher.fetch_and_process_new_data()  # No need to capture return as it's done in the Fetcher
        logger.info("New data fetched and cleaned successfully.")
    except Exception as e:
        logger.error(f"Error fetching new data: {e}")
        return

    # Ensure new data is available in Fetcher
    if fetcher.new_data_df is not None and not fetcher.new_data_df.empty:
        # Create Aggregator instance and pass the actual DataFrames
        aggregator = Aggregator(previous_data, fetcher.new_data_df)

        # Aggregate new data with master data
        try:
            aggregated_data = aggregator.aggregate_data()

            # Ensure aggregated data is not None before proceeding
            if aggregated_data is not None and not aggregated_data.empty:
                logger.info("Aggregated new data with master data successfully.")

                # Display the head of the aggregated data
                print("\nAggregated Data Head:")
                print(aggregated_data.head())  # Display the first few rows of the aggregated DataFrame

                # Perform calculations on the aggregated data
                processor = PerformCalculations(aggregated_data)
                try:
                    latest_data = processor.calculate_newdata(aggregated_data)  # Use the corrected method
                    print("\nCalculated Latest Data Head:")
                    print(latest_data.head())  # Display the first few rows of the calculated data

                    # Use DataFormatter to format the latest data before saving
                    formatter = DataFormatter()
                    latest_data = formatter.format_data(latest_data)
                    

                    # Check if there are rows to save
                    if not latest_data.empty:
                        # Save the latest data to the database with mode='replace'
                        try:
                            db_handler = DatabaseHandler()
                            db_handler.save_to_database(latest_data, table_name='ohlcv_marketcap_data', mode='replace')
                            logger.info("Latest data successfully saved to the database.")

                            # Fetch the latest 30 rows from the database table, sorted by Date in descending order
                            query = "SELECT * FROM ohlcv_marketcap_data ORDER BY Date DESC LIMIT 30"
                            
                            try:
                                latest_30_rows = pd.read_sql_query(query, db_handler.engine)  # Assuming db_handler has an 'engine' property for connection
                                print("\nLatest 30 Rows from the Database (Sorted by Date, Descending):")
                                print(latest_30_rows)  # Display the fetched rows
                                       
                                # Set Pandas display options to show all columns
                                pd.set_option('display.max_columns', None)  # Show all columns
                                pd.set_option('display.expand_frame_repr', False)  # Do not wrap the DataFrame when displaying

                                # Display all columns of the fetched rows
                                print("\nLatest 30 Rows from the Database (Sorted by Date, Descending):")
                                print(latest_30_rows)  # Print all columns and rows
                            
                            except Exception as fetch_error:
                                logger.error(f"Error fetching the latest 30 rows from the database: {fetch_error}")

                        except Exception as save_error:
                            logger.error(f"Error saving the latest data to the database: {save_error}")
                    else:
                        logger.warning("No new data available for the latest date to save.")

                except Exception as e:
                    logger.error(f"Error performing calculations on latest data: {e}")
            else:
                logger.warning("No aggregated data available for calculations.")
        except Exception as e:
            logger.error(f"Failed to aggregate new data with master data: {e}")
    else:
        logger.warning("No new data was fetched or it is empty.")

if __name__ == "__main__":
    main()