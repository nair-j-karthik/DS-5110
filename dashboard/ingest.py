import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Database connection string
DATABASE_URI = "postgresql://postgres:your_password@localhost:5432/sales_data"  # Replace 'your_password' with the actual password

# Define paths
BASE_PATH = "./data_source"
SALES_DATA_PATH = os.path.join(BASE_PATH, "sales_data")
CUSTOMER_DEMOGRAPHICS_PATH = os.path.join(BASE_PATH, "customer_demographics.csv")
HOLIDAYS_PATH = os.path.join(BASE_PATH, "holidays.csv")
LOCATIONS_PATH = os.path.join(BASE_PATH, "locations.csv")

def connect_to_database():
    """
    Create a database connection.
    """
    try:
        engine = create_engine(DATABASE_URI)
        connection = engine.connect()
        print("Connected to the PostgreSQL database successfully!")
        connection.close()  # Test connection and close it
        return engine
    except OperationalError as e:
        print("Failed to connect to the database.")
        print(e)
        exit(1)

def ingest_sales_data(engine):
    """
    Ingest sales data from 2018 to 2023 into the PostgreSQL database.
    """
    target_years = range(2018, 2024)  # 2018 to 2023
    target_months = [f"{month:02}" for month in range(1, 13)]  # January (01) to December (12)

    for year in target_years:
        year_path = os.path.join(SALES_DATA_PATH, str(year))
        if os.path.isdir(year_path):
            for month in target_months:
                month_path = os.path.join(year_path, month)
                if os.path.isdir(month_path):
                    for file in os.listdir(month_path):
                        if file.endswith(".csv"):
                            file_path = os.path.join(month_path, file)
                            print(f"Ingesting file: {file_path}")
                            sales_data = pd.read_csv(file_path)
                            sales_data.to_sql("sales", engine, if_exists="append", index=False)
    print("Sales data ingestion from 2018 to 2023 completed.")

def ingest_customer_demographics(engine):
    """
    Ingest customer demographics data into the PostgreSQL database.
    """
    print(f"Ingesting file: {CUSTOMER_DEMOGRAPHICS_PATH}")
    demographics_data = pd.read_csv(CUSTOMER_DEMOGRAPHICS_PATH)
    demographics_data.to_sql("customer_demographics", engine, if_exists="replace", index=False)
    print("Customer demographics ingestion completed.")

def ingest_locations(engine):
    """
    Ingest locations data into the PostgreSQL database.
    """
    print(f"Ingesting file: {LOCATIONS_PATH}")
    locations_data = pd.read_csv(LOCATIONS_PATH)
    locations_data.to_sql("locations", engine, if_exists="replace", index=False)
    print("Locations data ingestion completed.")

def ingest_holidays(engine):
    """
    Ingest holidays data into the PostgreSQL database.
    """
    print(f"Ingesting file: {HOLIDAYS_PATH}")
    holidays_data = pd.read_csv(HOLIDAYS_PATH)
    holidays_data.to_sql("holidays", engine, if_exists="replace", index=False)
    print("Holidays ingestion completed.")


def main():
    # Connect to the database
    engine = connect_to_database()
    
    # Ingest sales data for specific months
    ingest_sales_data(engine)
    
    # Ingest customer demographics and locations
    ingest_customer_demographics(engine)
    ingest_locations(engine)
    ingest_holidays(engine)

if __name__ == "__main__":
    main()
