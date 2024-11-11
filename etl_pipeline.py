# etl_pipeline.py
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Configurations for database connection and file paths
DATABASE_URI = 'sqlite:///sales_dashboard.db'
DATA_PATH = 'data/'
SALES_DATA_FILE = os.path.join(DATA_PATH, 'sales_data.csv')
DEMOGRAPHIC_DATA_FILE = os.path.join(DATA_PATH, 'demographic_data.csv')
WEATHER_DATA_FILE = os.path.join(DATA_PATH, 'weather_data.csv')
GOOGLE_TRENDS_FILE = os.path.join(DATA_PATH, 'google_trends.csv')

# Set up logging
logging.basicConfig(filename='etl_pipeline.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create a connection to the database
engine = create_engine(DATABASE_URI)

# Data cleaning and transformation functions
def clean_sales_data(df):
    """Cleans sales data by handling missing values and standardizing column formats."""
    df.dropna(inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    logging.info("Sales data cleaned.")
    return df

def clean_weather_data(df):
    """Processes weather data for ETL."""
    df['date'] = pd.to_datetime(df['current.dt'], unit='s')
    df = df[['date', 'current.temp', 'current.humidity']]
    df.columns = ['date', 'temperature', 'humidity']
    logging.info("Weather data cleaned.")
    return df

def load_to_database(df, table_name):
    """Loads a DataFrame into the specified database table."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data loaded into table: {table_name}")
    except Exception as e:
        logging.error(f"Failed to load data into table {table_name}: {e}")

# Main ETL function
def run_etl_pipeline():
    # Load raw data
    sales_df = pd.read_csv(SALES_DATA_FILE)
    demographics_df = pd.read_csv(DEMOGRAPHIC_DATA_FILE)
    weather_df = pd.read_csv(WEATHER_DATA_FILE)
    
    # Clean and transform data
    sales_df = clean_sales_data(sales_df)
    weather_df = clean_weather_data(weather_df)
    
    # Load transformed data into database
    load_to_database(sales_df, 'sales')
    load_to_database(weather_df, 'weather_data')
    load_to_database(demographics_df, 'customer_demographics')

if __name__ == "__main__":
    run_etl_pipeline()
