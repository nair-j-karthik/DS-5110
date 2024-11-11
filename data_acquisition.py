# data_acquisition.py
# Imports for data handling and API requests
import pandas as pd
import requests
from datetime import datetime, timedelta
from pytrends.request import TrendReq
import time
import logging
import os

# Set up logging to track progress and errors
logging.basicConfig(filename='data_acquisition.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configurations for file paths and API keys
DATA_PATH = 'data/'  # Directory to store downloaded data
OPENWEATHER_API_KEY = 'your_openweather_api_key'
SALES_DATA_FILE = os.path.join(DATA_PATH, 'sales_data.csv')
DEMOGRAPHIC_DATA_FILE = os.path.join(DATA_PATH, 'demographic_data.csv')
WEATHER_DATA_FILE = os.path.join(DATA_PATH, 'weather_data.csv')
GOOGLE_TRENDS_FILE = os.path.join(DATA_PATH, 'google_trends.csv')

# Function to ensure data directory exists
def ensure_data_directory():
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)
        logging.info("Data directory created.")

# Load sales data from CSV
def load_sales_data():
    try:
        sales_data = pd.read_csv(SALES_DATA_FILE)
        logging.info("Sales data loaded successfully.")
        return sales_data
    except FileNotFoundError:
        logging.error("Sales data file not found.")
        return None

# Load customer demographics data
def load_demographics_data():
    try:
        demographics_data = pd.read_csv(DEMOGRAPHIC_DATA_FILE)
        logging.info("Demographic data loaded successfully.")
        return demographics_data
    except FileNotFoundError:
        logging.error("Demographic data file not found.")
        return None

# Fetch historical weather data for a specific location
def fetch_weather_data(lat, lon, start_date, days=7):
    weather_data = []
    for i in range(days):
        date = start_date - timedelta(days=i)
        timestamp = int(datetime.timestamp(date))
        url = f"http://api.openweathermap.org/data/2.5/onecall/timemachine"
        params = {
            'lat': lat,
            'lon': lon,
            'dt': timestamp,
            'appid': OPENWEATHER_API_KEY
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            weather_data.append(response.json())
            logging.info(f"Weather data for {date} fetched successfully.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch weather data for {date}: {e}")
        time.sleep(1)  # Pause to avoid API rate limits
    return pd.json_normalize(weather_data)

# Fetch Google Trends data for specified keywords
def fetch_google_trends(keywords, timeframe='today 3-m'):
    pytrends = TrendReq()
    try:
        pytrends.build_payload(keywords, cat=0, timeframe=timeframe)
        trends_data = pytrends.interest_over_time()
        logging.info("Google Trends data fetched successfully.")
        return trends_data
    except Exception as e:
        logging.error(f"Failed to fetch Google Trends data: {e}")
        return None

# Save DataFrames to CSV
def save_data_to_csv(dataframe, filename):
    if dataframe is not None:
        dataframe.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}.")
    else:
        logging.warning(f"No data to save for {filename}.")

# Run the data acquisition steps
if __name__ == "__main__":
    ensure_data_directory()
    # Load and save local data
    sales_data = load_sales_data()
    demographics_data = load_demographics_data()
    
    # Fetch and save weather data
    start_date = datetime.now()
    weather_df = fetch_weather_data(lat=40.7128, lon=-74.0060, start_date=start_date, days=7)
    save_data_to_csv(weather_df, WEATHER_DATA_FILE)
    
    # Fetch and save Google Trends data
    trends_df = fetch_google_trends(['retail', 'holiday shopping'])
    save_data_to_csv(trends_df, GOOGLE_TRENDS_FILE)
