import os
import time
import pandas as pd
import logging
import re  
from datetime import datetime, timedelta
from selenium import webdriver  # Ensure you import the webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.database_handler import DatabaseHandler  # Ensure this import is correct


# Set up logger for the data_loader module
logger = logging.getLogger('data_fetcher_logger')
logger.setLevel(logging.INFO)

log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'
log_file_path = os.path.join(log_directory, 'data_fetcher.log')
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



# Constants
DOWNLOAD_FOLDER = 'C:\\Users\\46704\\Downloads'
TIME_LIMIT = 10 * 60  # 10 minutes
CRYPTOS = [
    'avalanche', 'binance-coin', 'bitcoin', 'bitcoin-cash', 'cardano', 
    'chainlink', 'dogecoin', 'ethereum', 'kaspa', 'lido-staked-ether', 
    'litecoin', 'near-protocol', 'polkadot', 'ripple', 'shiba-inu', 
    'solana', 'tether', 'toncoin', 'tron', 'uniswap', 'usd-coin', 
    'weth', 'wrapped-bitcoin', 'wrapped-steth'
]

class NewDataLoader:
    """Class responsible for downloading and processing cryptocurrency data."""

    def __init__(self, download_folder=DOWNLOAD_FOLDER, time_limit=TIME_LIMIT, cryptos=CRYPTOS):
        """Initialize the NewDataLoader with specified parameters."""
        self.download_folder = download_folder
        self.time_limit = time_limit
        self.cryptos = cryptos
        self.driver = self.create_driver()
        self.downloaded_files = {}  # Dictionary to store crypto names and file paths
        self.crypto_names = []  # List to store cryptocurrency names

    def create_driver(self):
        """Create and configure the WebDriver."""
        edge_options = Options()
        edge_options.add_argument("--headless")  # Run in headless mode
        service = Service('C:\\Users\\46704\\Downloads\\edgedriver_win64\\msedgedriver.exe')
        driver = webdriver.Edge(service=service, options=edge_options)
        logger.info("WebDriver created successfully.")
        return driver

    def click_export_button(self, crypto):
        """Click the 'Export' button for the given cryptocurrency and track the file."""
        url = f'https://coincodex.com/crypto/{crypto}/historical-data/'
        self.driver.get(url)
        
        try:
            wait = WebDriverWait(self.driver, 10)
            export_button = wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.export.link.button.button-secondary"))
            )
            self.driver.execute_script("arguments[0].scrollIntoView(true);", export_button)
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.export.link.button.button-secondary")))
            self.driver.execute_script("arguments[0].click();", export_button)
            time.sleep(5)  # Allow time for the CSV to be generated and downloaded
            logger.info(f"Export button clicked for {crypto}.")

            # After downloading, get the most recent CSV file in the download folder
            recent_file = max(self.get_recent_csv_files(), key=os.path.getctime)  # Get the latest file
            self.downloaded_files[crypto] = recent_file  # Store the file path associated with the crypto

            # Store the cryptocurrency name for later use
            self.crypto_names.append(crypto.capitalize())  # Add name to the list

            return crypto.capitalize()  # Return the name for logging purposes

        except Exception as e:
            logger.error(f"Error finding or clicking the export button for {crypto}: {e}")
            return None

    def get_recent_csv_files(self):
        """Get CSV files modified in the last TIME_LIMIT."""
        now = time.time()
        recent_files = [
            os.path.join(self.download_folder, f)
            for f in os.listdir(self.download_folder)
            if f.endswith('.csv') and (now - os.path.getmtime(os.path.join(self.download_folder, f)) <= self.time_limit)
        ]
        logger.info(f"Found {len(recent_files)} recent CSV files.")
        return recent_files
    
    @staticmethod
    def filter_rows_by_date(file_path):
        """Filter rows where the 'Start' column equals yesterday's date."""
        try:
            df = pd.read_csv(file_path)
            if 'Start' not in df.columns:
                logger.warning(f"Missing 'Start' column in {file_path}")
                return None
            
            yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            filtered_df = df[df['Start'] == yesterday_date].copy()
            
            if not filtered_df.empty:
                logger.info(f"Filtered data from {file_path} for date {yesterday_date}.")
                return filtered_df
            else:
                logger.warning(f"No data for yesterday's date in {file_path}.")
                return None
        except Exception as e:
            logger.error(f"Error processing CSV file {file_path}: {e}")
            return None

    @staticmethod
    def extract_crypto_name_from_filename(filename):
        """Extract the cryptocurrency name from the downloaded CSV filename."""
        return filename.split('_', 1)[0].lower()  # Return the part before the first underscore in lowercase

    @staticmethod
    def rename_and_clean_dataframe(df):
        """Rename 'Start' to 'Date' and drop the 'End' column."""
        if 'Start' in df.columns:
            df.rename(columns={'Start': 'Date'}, inplace=True)
            logger.info("Renamed 'Start' column to 'Date'.")
        
        if 'End' in df.columns:
            df.drop(columns=['End'], inplace=True)
            logger.info("Dropped 'End' column.")
        
        return df  
    
    @staticmethod
    def delete_csv_files(files):
        """Delete the specified CSV files."""
        for file_path in files:
            try:
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

    def combine_filtered_data(self, recent_csv_files):
        """Combine filtered rows from recent CSV files into a single DataFrame."""
        combined_df = pd.DataFrame()  # Empty DataFrame to store all filtered rows

        for csv_file in recent_csv_files:
            filtered_df = self.filter_rows_by_date(csv_file)
            if filtered_df is not None:
                # Extract the cryptocurrency name from the filename
                crypto_name = self.extract_crypto_name_from_filename(os.path.basename(csv_file))
                if crypto_name:
                    filtered_df['CryptocurrencyName'] = crypto_name  # Add the name to the DataFrame
                combined_df = pd.concat([combined_df, filtered_df], ignore_index=True)

        logger.info(f"Total rows combined: {len(combined_df)}")
        return combined_df

    def process_crypto_data(self):
        """Main process to download and combine cryptocurrency data."""
        try:
            # Step 1: Click export buttons for all cryptos
            for crypto in self.cryptos:
                self.click_export_button(crypto)

            # Step 2: Collect recent CSV files
            recent_csv_files = self.get_recent_csv_files()

            # Step 3: Combine filtered rows into a single DataFrame
            combined_df = self.combine_filtered_data(recent_csv_files)

            # Optionally delete the CSV files after processing
            if recent_csv_files:
                self.delete_csv_files(recent_csv_files)  # Delete after processing if desired

            if not combined_df.empty:
                logger.info("Combined DataFrame successfully created.")
                return combined_df, self.crypto_names  # Return the combined DataFrame and names
            else:
                logger.warning("No data to combine.")
                return None, self.crypto_names

        finally:
            # Ensure that the driver is closed regardless of success or failure
            self.driver.quit()
            logger.info("WebDriver closed.")

# Create an instance of DatabaseHandler
db_handler = DatabaseHandler()  # Make sure this class is properly defined

class FetchedDataProcessor:
    """A class to handle the entire process of fetching, filtering, and saving crypto data."""

    def __init__(self, loader):
        self.loader = loader
        self.db_handler = db_handler  # Add the db_handler here

    def execute(self):
        """Execute the entire data loading and processing workflow."""
        combined_df, crypto_names = self.loader.process_crypto_data()  # Get the combined DataFrame

        if combined_df is not None:
            # Step 1: Transform the DataFrame
            transformed_df = self.transform_csv(combined_df)
        
            if transformed_df is not None:
                # Step 2: Save transformed DataFrame to the database
                self.db_handler.save_to_database(transformed_df, 'ohlcv_marketcap_data')  # Save to a specified table
                logger.info("Transformed DataFrame saved to database successfully.")
                return transformed_df
            else:
                logger.error("Data transformation failed. No data to save.")
                return None
        else:
            logger.error("No combined data to save.")
            return None

    @staticmethod
    def transform_csv(df):
        """Transform the DataFrame by renaming columns and cleaning."""
        df = NewDataLoader.rename_and_clean_dataframe(df)  # Use the method from NewDataLoader
        return df

if __name__ == "__main__":
    # Create an instance of the NewDataLoader
    data_loader = NewDataLoader()
    
    # Create an instance of FetchedDataProcessor
    fetched_data_processor = FetchedDataProcessor(data_loader)
    
    # Execute the data processing
    transformed_data = fetched_data_processor.execute()  # This should now be recognized properly
    
    if transformed_data is not None:
        logger.info("Data processed and saved successfully.")
    else:
        logger.error("Data processing failed.")