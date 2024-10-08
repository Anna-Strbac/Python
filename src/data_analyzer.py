import os
import pandas as pd
import numpy as np
import logging
import json
from sqlalchemy import create_engine
from src.database_handler import DatabaseHandler

# Set up logger for the data_loader module
logger = logging.getLogger('data_analyzer_logger')
logger.setLevel(logging.INFO)

log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'
log_file_path = os.path.join(log_directory, 'data_analyzer.log')
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class DataAnalyzer:
    """ 
    A class to make calculations on cryptocurrency market data.
    
    Methods:
    calculate_typical_price(): Calculates the typical price based on the open, high, low, and close prices.
    calculate_vwap(): Calculates the Volume Weighted Average Price (VWAP) for each cryptocurrency.
    determine_thresholds(percentile=98): Determines price change thresholds based on percentage changes.
    clean_data(): Drops unnecessary columns from the DataFrame.
    calculate_price_change(): Calculates daily price changes and percentage changes for each cryptocurrency.
    detect_large_changes(thresholds, data_subset=None): Detects large changes in percentage based on the provided thresholds.
    """
    
    def __init__(self, df):
        if df is None or df.empty:
            raise ValueError("DataFrame cannot be None or empty.")
        self.df = df 

    def calculate_typical_price(self):
        """Calculate the Typical Price."""
        self.df['Typical_Price'] = (self.df['High'] + self.df['Low'] + self.df['Close'] + self.df['Open']) / 4
        logger.info("Calculated Typical Price.")
        return self.df
    

    def calculate_vwap(self):
        """Calculate VWAP for each cryptocurrency."""
        self.df['Cumulative_Volume'] = self.df.groupby('CryptocurrencyName')['Volume'].cumsum()
        self.df['VWAP'] = np.nan

        for crypto in self.df['CryptocurrencyName'].unique():
            crypto_data = self.df[self.df['CryptocurrencyName'] == crypto].copy()
            crypto_data['VWAP'] = (crypto_data['Typical_Price'] * crypto_data['Volume']).cumsum() / crypto_data['Cumulative_Volume']
            self.df.loc[crypto_data.index, 'VWAP'] = crypto_data['VWAP']

        self.df.drop(columns=['Cumulative_Volume'], inplace=True)
        logger.info("Calculated VWAP for each cryptocurrency.")
        return self.df

    def determine_thresholds(self, percentile=98):
        """Determine price change thresholds for percentage changes."""
        logger.info(f"Determining price change thresholds at the {percentile}th percentile.")
        thresholds = {column: {} for column in ['Open_Pct_Change', 'High_Pct_Change', 'Low_Pct_Change', 'Close_Pct_Change', 'Volume_Pct_Change']}

        for crypto in self.df['CryptocurrencyName'].unique():
            crypto_df = self.df[self.df['CryptocurrencyName'] == crypto]

            for column in ['Open', 'High', 'Low', 'Close', 'Volume']:
                pct_change = crypto_df[column].pct_change() * 100
                valid_pct_change = pct_change[~pct_change.isna()]

                if not valid_pct_change.empty:
                    threshold = np.percentile(valid_pct_change, percentile)
                    thresholds[f'{column}_Pct_Change'][crypto] = threshold
                    logger.info(f"Threshold for {crypto} - {column}_Pct_Change: {threshold:.2f}")
                else:
                    thresholds[f'{column}_Pct_Change'][crypto] = np.nan
                    logger.warning(f"No valid data for {crypto} - {column}_Pct_Change. Threshold set to NaN.")

        return thresholds
    
    def clean_data(self):
        """Drop specified columns and save the resulting DataFrame to a CSV file."""
        columns_to_drop = ['Open_Daily_Change', 'High_Daily_Change', 'Low_Daily_Change', 'Close_Daily_Change', 'Volume_Daily_Change']
        self.df.drop(columns=columns_to_drop, inplace=True, errors='ignore')  # Use self.df instead of self.master_df
        logger.info("Dropped unnecessary columns from the DataFrame.")
    
        return self
    
    def calculate_price_change(self):
        """Calculate the daily price change and percentage change for each cryptocurrency."""
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df = self.df.sort_values(by=['CryptocurrencyName', 'Date'])

        self.df[['Open_Daily_Change', 'High_Daily_Change', 'Low_Daily_Change', 'Close_Daily_Change', 'Volume_Daily_Change']] = self.df.groupby('CryptocurrencyName')[['Open', 'High', 'Low', 'Close', 'Volume']].diff()

        self.df[['Open_Daily_Pct_Change', 'High_Daily_Pct_Change', 'Low_Daily_Pct_Change', 'Close_Daily_Pct_Change', 'Volume_Pct_Change']] = self.df.groupby('CryptocurrencyName')[['Open', 'High', 'Low', 'Close', 'Volume']].pct_change() * 100
        
        return self.df

    def detect_large_changes(self, thresholds, data_subset=None):
        """Detect rows where percentage change exceeds the given thresholds.

        Args:
            thresholds (dict): A dictionary with the threshold values for each cryptocurrency.
            data_subset (DataFrame, optional): A subset of the data to analyze. If None, use self.df.
        """
        # If a data_subset is provided, use it; otherwise, use the full DataFrame (self.df)
        df_to_analyze = data_subset if data_subset is not None else self.df

        large_changes = df_to_analyze[
            (df_to_analyze['Open_Daily_Pct_Change'].abs() > df_to_analyze['CryptocurrencyName'].map(thresholds['Open_Pct_Change'])) |
            (df_to_analyze['High_Daily_Pct_Change'].abs() > df_to_analyze['CryptocurrencyName'].map(thresholds['High_Pct_Change'])) |
            (df_to_analyze['Low_Daily_Pct_Change'].abs() > df_to_analyze['CryptocurrencyName'].map(thresholds['Low_Pct_Change'])) |
            (df_to_analyze['Close_Daily_Pct_Change'].abs() > df_to_analyze['CryptocurrencyName'].map(thresholds['Close_Pct_Change'])) |
            (df_to_analyze['Volume_Pct_Change'].abs() > df_to_analyze['CryptocurrencyName'].map(thresholds['Volume_Pct_Change']))
        ]

        logger.info(f"Detected {len(large_changes)} large changes in data.")
        return large_changes

   
class PerformCalculations:
    """
    A class to perform calculations on cryptocurrency data, including master data and new data.

    Methods:
    calculate_masterdata(): Executes all necessary calculations on the master data, including typical price, VWAP, and thresholds.
    save_thresholds(thresholds): Saves calculated thresholds to a JSON file for later use.
    load_thresholds(): Loads thresholds from a JSON file to be used in analysis.
    calculate_newdata(aggregated_data): Runs calculations on new data loaded from the database, detecting large changes on new data.
    display_large_changes(large_changes, data_source): Displays rows where large changes were detected in the specified data source.
    """
    def __init__(self, master_df, new_data_df=None):
        if master_df is None or master_df.empty:
            raise ValueError("Master DataFrame cannot be None or empty.")
        self.master_df = master_df
        self.new_data_df = new_data_df
        self.db_handler = DatabaseHandler('sqlite:///"C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/cryptocurrency_db.db')

    def calculate_masterdata(self):
        """Run all the necessary calculations on the master data."""
        master_analyzer = DataAnalyzer(self.master_df)
        master_analyzer.calculate_typical_price()
        master_analyzer.calculate_vwap()   
    
        if master_analyzer.df.empty:
            logger.error("Master DataFrame is empty after calculating typical price and VWAP. Exiting.")
            return None
    
        thresholds = master_analyzer.determine_thresholds(percentile=98)
        if not thresholds:
            logger.error("No thresholds calculated, aborting further analysis.")
            return None

        # Save the calculated thresholds
        self.save_thresholds(thresholds)
            
        master_analyzer.calculate_price_change()
        master_analyzer.clean_data()

        logger.info("All calculations performed successfully.")
        return master_analyzer.df


    def save_thresholds(self, thresholds):
        """Save thresholds to a JSON file."""
        with open('thresholds.json', 'w') as f:
            json.dump(thresholds, f)
        logger.info("Thresholds saved to 'thresholds.json'.")


    def load_thresholds(self):
        """Load thresholds from a JSON file."""
        try:
            with open('thresholds.json', 'r') as f:
                thresholds = json.load(f)
            logger.info("Thresholds loaded from 'thresholds.json'.")
            return thresholds
        except FileNotFoundError:
            logger.error("Thresholds file not found. Exiting.")
            return None      

    def calculate_newdata(self, aggregated_data):
        """Run all the necessary calculations on the new data loaded from the database."""
        # Create a DataAnalyzer instance with the aggregated data
        newdata_analyzer = DataAnalyzer(aggregated_data)
    
        # Perform calculations
        newdata_analyzer.calculate_typical_price()
        newdata_analyzer.calculate_vwap()

        # Load thresholds from the saved file
        thresholds = self.load_thresholds()
        if thresholds is None:
            logger.error("Cannot proceed without thresholds.")
            return None

        newdata_analyzer.calculate_price_change()
        newdata_analyzer.clean_data()

        # Filter the data for the last two days
        last_two_days = aggregated_data[aggregated_data['Date'] >= (pd.Timestamp.now() - pd.Timedelta(days=2))]

        # Check if there is data for the last two days
        if last_two_days.empty:
            logger.warning("No data available for the last three days.")
            return None

        # Detect large changes in the last three days
        large_changes = newdata_analyzer.detect_large_changes(thresholds, last_two_days)

        # Display large changes specific to the last two days
        self.display_large_changes(large_changes, "New Data (Last 2 Days)")

        return newdata_analyzer.df  # Return the processed DataFrame
    
    
    def display_large_changes(self, large_changes, data_source):
        """Display rows where large changes were detected."""
        if not large_changes.empty:
            logger.warning(f"Displaying rows with large changes for {data_source}.")
            print(f"Rows with large changes detected in {data_source}:")
            print(large_changes[['Date', 'CryptocurrencyName', 'Open', 'High', 'Low', 'Close', 'Volume', 
                                 'Open_Daily_Pct_Change', 'High_Daily_Pct_Change', 'Low_Daily_Pct_Change', 
                                 'Close_Daily_Pct_Change', 'Volume_Pct_Change']])
        else:
            logger.info(f"No large changes detected in {data_source}.")
            print(f"No large changes detected in {data_source}.")
