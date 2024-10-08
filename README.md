# Cryptocurrency Data Processing Application

This application is designed to load historical OHLCV (Open, High, Low, Close, Volume) and market cap data for cryptocurrencies from CSV files and save it to a SQLite database. It then fetches new data daily using the Selenium library in Python, scheduled through Windows Task Scheduler.

Throughout the project's steps, the data is processed, cleaned, and new calculations are applied to analyze cryptocurrency market data. The application retrieves data from a source, aggregates it with existing data in the database, performs calculations, and saves the results back to the database. The application is built using Python and utilizes several libraries for data handling and logging.

## Table of Contents

- Features
- Installation
- Project Structure
- Logging
- Contributing
- License
- Contact

## Features

data_loader: Efficiently loads historical OHLCV and market cap data for various cryptocurrencies from CSV files into a SQLite database.

data_fetcher: Automatically retrieves new cryptocurrency data on a daily basis using the Selenium library, allowing for up-to-date market information.

data_source: Combines new data with existing records in the database, ensuring that all information is consolidated for analysis.

data_cleaner: Processes the fetched data to ensure quality and relevance, including cleaning and formatting operations.

data_analyzer: Applies a variety of calculations to the data.

database_handler: Utilizes SQLite for data storage, providing a lightweight and efficient solution for managing historical cryptocurrency data.

logger: Implements comprehensive logging throughout the application to track the data loading, processing, and saving operations, aiding in troubleshooting and analysis.

run_skript.bat: Configured to run seamlessly with Windows Task Scheduler, allowing for hands-free operation and automated data updates.

## Installation

Follow these steps to install and set up the project on your local machine:

Prerequisites
Before you begin, ensure you have the following installed:

Python 3.9.18
pip 24.2
SQLite (comes pre-installed with Python)

Step 1: Clone the Repository
Clone the project repository to your local machine using the following command:

Step 2: Install Required Libraries
Install the necessary Python libraries by running:
pip install -r requirements.txt

Step 3: Set Up WebDriver
1. Download the appropriate WebDriver for your browser.
2. Extract the WebDriver executable and place it in a directory of your choice.
3. Update the Service path in the code to point to the location of the WebDriver exec

Step 4: Configure Download Folder
Set the download folder where new CSV files will be saved and loaded through Selenium.

Step 5: Configure Logging Directory
Create a directory for logs if it doesn’t already exist. You can specify the log directory in your script.

Step 6: Set Up the Database
The application uses an SQLite database. Specify the database file path in your script.

Step 7: Run the Application
You can run the application by executing the following command:
python main.py

Step 8: Schedule Daily Fetching
You can run the application daily using Windows Task Scheduler. Add run_script.bat to the Task Scheduler.

Step 9: Automated Tests
You can run tests automatically with run-tests.ps1, which will execute tests if you change the structure of the scripts.

## Logging
The application uses Python's logging library to track operations and errors. Logs are stored in the logs directory. You can adjust the logging level in the code to capture more detailed information.

## Contributing
Contributions are welcome! To contribute:

Fork the repository.
Create a new branch.
Make your changes and commit them.
Push to your branch.
Open a pull request.
Please follow the existing code style and include tests where applicable.

## Licence
This project is licensed under the MIT License. See the LICENSE file for details.

## Contact
Name: Anna Strbac
Email: ann.strbac@gmail.com
GitHub: Anna-Strbac
