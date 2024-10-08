import os
import pandas as pd
import logging
from src.database_handler import DatabaseHandler


# Set up logger for the data_loader module
logger = logging.getLogger('data_cleaner_logger')
logger.setLevel(logging.INFO)

log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'
log_file_path = os.path.join(log_directory, 'data_cleaner.log')
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class DataCleaner:
    def __init__(self, df, numeric_columns=None, date_columns=None):
        self.df = df
        # Use provided numeric columns or default ones
        self.numeric_columns = numeric_columns if numeric_columns else ['Market Cap', 'Volume', 'Open', 'High', 'Low', 'Close']
        self.date_columns = date_columns if date_columns else ['Date']  # Default to 'Date' column
        self.db_handler = DatabaseHandler()

    def clean_data(self):
        """Main function to clean data."""
        volume_na_count, forward_fill_count = self.validate_and_clean_data()
        self.remove_duplicates()
        logger.info("Data cleaning completed.")
        
        # Print the cleaned DataFrame and number of rows
        self.print_cleaned_data()

        # Log the counts of cleaned rows
        logger.info(f"Rows set to NA in 'Volume': {volume_na_count}")
        logger.info(f"Rows forward filled in 'Volume': {forward_fill_count}")
 
        # Save the cleaned data to the database
        self.save_cleaned_data()

        return self.df  # Return the cleaned DataFrame
        
    def remove_duplicates(self):
        """Remove duplicate rows."""
        duplicates = self.df.duplicated().sum()
        if duplicates > 0:
            self.df = self.df.drop_duplicates()
            logger.info(f"Removed {duplicates} duplicate rows after cleaning.")
        else:
            logger.info("No duplicate rows found.")

    def validate_and_clean_data(self):
        """Validate and clean the data formats."""
        logger.info("Validating and cleaning data.")
        volume_na_count = 0  # Count of rows set to NA in 'Volume'
        forward_fill_count = 0  # Count of rows forward filled in 'Volume'

        # Check for missing values in 'Date' and 'CryptocurrencyName' columns
        if self.df['Date'].isna().any():
            logger.warning("NaN values found in 'Date' column.")
        if self.df['CryptocurrencyName'].isna().any():
            logger.warning("NaN values found in 'CryptocurrencyName' column.")

        # Check for negative, zero, and missing values in numeric columns
        for column in self.numeric_columns:
            if column in self.df.columns:
                # Check for missing values
                if self.df[column].isna().any():
                    logger.warning(f"Missing values found in '{column}' column.")

                # Check for negative or zero values in Open, High, Low, Close
                if column in ['Open', 'High', 'Low', 'Close']:
                    if (self.df[column] <= 0).any():
                        logger.warning(f"Zero or negative values found in '{column}' column.")

                # Handle cleaning for 'Market Cap' column
                if column == 'Market Cap':
                    self.df[column] = self.df[column].replace({-1: pd.NA})  # Assuming -1 is an example of a negative value
                    self.df[column] = self.df[column].where(self.df[column] > 0, pd.NA)  # Set any negative values to NA
                    logger.info(f"Replaced negative values in 'Market Cap' with NA.")

                # Handle cleaning for 'Volume' column
                if column == 'Volume':
                    # Initial count of NA values before cleaning
                    initial_na_count = self.df[column].isna().sum()

                    # Set negative and zero values to NA
                    self.df[column] = self.df[column].where(self.df[column] > 0, pd.NA)
                    volume_na_count = self.df[column].isna().sum() - initial_na_count  # Count new NA values in 'Volume'

                    logger.info(f"Rows set to NA in 'Volume' during cleaning: {volume_na_count}")

                    # Fill zero or NA values using forward fill
                    self.df['Date'] = pd.to_datetime(self.df['Date'])
                    self.df.sort_values(by=['CryptocurrencyName', 'Date'], inplace=True)

                    # Count the number of rows to forward fill
                    forward_fill_indexes = self.df[self.df[column].isna()].index
                    forward_fill_count = len(forward_fill_indexes)  # Count the rows that will be forward filled
                    
                    # Perform forward fill
                    self.df[column] = self.df.groupby('CryptocurrencyName')[column].ffill()
                    
                    # After forward fill, calculate how many were actually filled
                    remaining_na_after_fill = self.df[self.df[column].isna()].index.isin(forward_fill_indexes).sum()
                    forward_fill_count -= remaining_na_after_fill  # Update the forward fill count
                    
                    logger.info(f"Rows forward filled in 'Volume' column: {forward_fill_count}")

            else:
                logger.warning(f"Column '{column}' is missing in the data.")

        self.check_and_convert_formats()
        return volume_na_count, forward_fill_count  # Return counts of NA and forward filled rows

    def check_and_convert_formats(self):
        """Check and convert numeric, date, and text formats."""
        for column in self.numeric_columns:
            if column in self.df.columns:
                if pd.api.types.is_numeric_dtype(self.df[column]):
                    logger.info(f"Column '{column}' is already in numeric format.")
                else:
                    try:
                        self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
                        logger.info(f"Column '{column}' successfully converted to numeric format.")
                    except Exception as e:
                        logger.error(f"Failed to convert column '{column}' to numeric format: {e}")

        for column in self.date_columns:
            if column in self.df.columns:
                if pd.api.types.is_datetime64_any_dtype(self.df[column]):
                    logger.info(f"Column '{column}' is already in date format.")
                else:
                    try:
                        self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
                        logger.info(f"Column '{column}' successfully converted to date format.")
                    except Exception as e:
                        logging.error(f"Failed to convert column '{column}' to date format: {e}")

    def print_cleaned_data(self):
        """Print the cleaned DataFrame and the total row count."""
        print("Cleaned DataFrame:")
        print(self.df.head())  # Print the first few rows of the cleaned DataFrame
        print(f"\nTotal number of rows in cleaned DataFrame: {len(self.df)}") 
        
    def save_cleaned_data(self):
        """Save the cleaned DataFrame to the database."""
        try:
            self.db_handler.save_to_database(self.df, table_name='ohlcv_marketcap_data')  # Save to 'ohlcv_marketcap_data' table
            logger.info("Cleaned data successfully saved to the database.")
        except Exception as e:
            logger.error(f"Error saving data to SQLite: {str(e)}")


class PerformCleaning:
    def __init__(self, df):
        self.df = df

    def clean_all(self):
        """Clean the dataframe using the DataCleaner class."""
        # Check if the input DataFrame is valid before proceeding
        if self.df is None or self.df.empty:
            logger.error("Input DataFrame is None or empty. Cannot perform cleaning.")
            return None  # Early return

        # Initialize the DataCleaner with the dataframe
        cleaner = DataCleaner(self.df)
        # Perform all cleaning operations
        cleaned_df = cleaner.clean_data()  # Now captures the cleaned DataFrame

        # Check if the cleaned DataFrame is valid
        if cleaned_df is not None and not cleaned_df.empty:
            logger.info("Cleaning completed successfully.")
        else:
            logger.error("Cleaned DataFrame is None or empty. Cannot proceed further.")

        # Return the cleaned dataframe
        return cleaned_df  # Return the cleaned DataFrame directly
    
    
    
class DataFormatter:
    """
    A class to format numerical and percentage columns in a DataFrame.
    Formats percentage columns with a '%' sign and ensures numeric columns have four decimal places.
    """
    
    def __init__(self):
        # Define columns that require specific formatting
        self.percentage_cols = [
            'Open_Daily_Pct_Change', 
            'High_Daily_Pct_Change', 
            'Low_Daily_Pct_Change', 
            'Close_Daily_Pct_Change', 
            'Volume_Pct_Change'
        ]
        self.numeric_cols = [
            'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap', 
            'Typical_Price', 'VWAP'
        ]
        
    def format_percentages(self, df):
        """
        Format percentage columns to strings with four decimal places followed by a '%' sign.
        
        Parameters:
        df (pd.DataFrame): The DataFrame to format.
        
        Returns:
        pd.DataFrame: The DataFrame with formatted percentage columns.
        """
        try:
            df[self.percentage_cols] = df[self.percentage_cols].applymap(lambda x: f"{x:.4f}%")
            logger.info("Formatted percentage columns.")
        except Exception as e:
            logger.error(f"Error formatting percentage columns: {e}")
        return df

    def format_numerics(self, df):
        """
        Format numeric columns to strings with four decimal places.
        
        Parameters:
        df (pd.DataFrame): The DataFrame to format.
        
        Returns:
        pd.DataFrame: The DataFrame with formatted numeric columns.
        """
        try:
            df[self.numeric_cols] = df[self.numeric_cols].applymap(lambda x: f"{x:.4f}")
            logger.info("Formatted numeric columns.")
        except Exception as e:
            logger.error(f"Error formatting numeric columns: {e}")
        return df

    def format_date(self, df):
        """
        Format the date column to a string format.
        
        Parameters:
        df (pd.DataFrame): The DataFrame to format.
        
        Returns:
        pd.DataFrame: The DataFrame with formatted date column.
        """
        try:
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            logger.info("Formatted date column.")
        except Exception as e:
            logger.error(f"Error formatting date column: {e}")
        return df

    def format_data(self, df):
        """
        Format the DataFrame by applying all formatting functions to the DataFrame.
        
        Parameters:
        df (pd.DataFrame): The DataFrame to format.
        
        Returns:
        pd.DataFrame: The formatted DataFrame.
        """
        logger.info("Formatting DataFrame.")
        df = self.format_percentages(df)
        df = self.format_numerics(df)
        df = self.format_date(df)
        logger.info("DataFrame formatting completed.")
        return df
