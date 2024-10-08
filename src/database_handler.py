import pandas as pd
from sqlalchemy import create_engine, exc
import logging
import os
import sqlite3

# Set up logger for the data_loader module
logger = logging.getLogger('database_handler_logger')
logger.setLevel(logging.INFO)

log_directory = 'C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/logs'
log_file_path = os.path.join(log_directory, 'database_handler.log')
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class DatabaseHandler:
    def __init__(self, database_url='C:/Users/46704/Desktop/Kunskapskontroll 2 Python/Project/cryptocurrency_db.db'):
        """Initialize the DatabaseHandler with the provided database URL."""
        self.database_url = os.path.abspath(database_url)  # Ensure the path is absolute
        self.engine = create_engine(f'sqlite:///{self.database_url}')
        logger.info(f"DatabaseHandler initialized with database URL: {self.database_url}")

    def save_to_database(self, df: pd.DataFrame, table_name: str, mode: str = 'append'):
        """Save the DataFrame to the specified table in the database."""
        if mode not in ['replace', 'append']:
            logger.error("Invalid mode. Use 'replace' or 'append'.")
            raise ValueError("Invalid mode. Use 'replace' or 'append'.")

        try:
            df.to_sql(table_name, con=self.engine, if_exists=mode, index=False)
            logger.info(f"Data saved to table '{table_name}' successfully in '{mode}' mode.")
        except Exception as e:
            logger.error(f"Error saving data to the database: {e}")

    def load_data_from_database(self, table_name: str = 'ohlcv_marketcap_data') -> pd.DataFrame:
        """Load data from the SQLite database into a DataFrame."""
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql_table(table_name, con=connection)
                logger.info(f"Data loaded from table '{table_name}'.")
                return df
        except exc.OperationalError as e:
            logger.error(f"OperationalError while loading data from '{table_name}': {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error
        except Exception as e:
            logger.error(f"Error loading from database: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame."""
        try:
            with self.engine.connect() as connection:
                result = pd.read_sql_query(query, connection)
                logger.info("SQL query executed successfully.")
                return result
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    def close(self):
        """Close the database engine connection."""
        if self.engine:
            self.engine.dispose()  # Dispose of the engine when done
            logger.info("Database connection closed.")

    def get_last_n_rows(self, table_name: str, n: int) -> pd.DataFrame:
        """Fetch the last n rows from a specific table in the database."""
        try:
            with sqlite3.connect(self.database_url) as conn:
                query = f"SELECT * FROM {table_name} ORDER BY Date DESC LIMIT {n};"
                logger.debug(f"Executing query to get last {n} rows from '{table_name}'.")
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Error fetching last {n} rows from '{table_name}': {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    def display_last_n_rows(self, table_name: str = 'ohlcv_marketcap_data', n: int = 30):
        """Display the last n rows from the specified table in descending order."""
        data = self.get_last_n_rows(table_name, n)

        if not data.empty:
            logger.info(f"Displaying the last {n} rows from table '{table_name}'.")
            print(f"Last {n} Rows from table '{table_name}' in Descending Order:")
            print(data.head(n))  # Display the first n rows (which are the latest n rows)
        else:
            logger.warning(f"Failed to load data from table '{table_name}' or no data available.")
           
           
