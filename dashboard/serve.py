import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Database connection
DATABASE_URI = "postgresql://postgres:your_password@localhost:5432/sales_data"  # Replace with your credentials
engine = create_engine(DATABASE_URI)

def get_daily_data(day):
    """
    Fetch sales data for a specific day.
    """
    query = f"""
    SELECT * FROM sales
    WHERE date = '{day}';
    """
    return pd.read_sql(query, engine)

def get_weekly_data(start_date, end_date):
    """
    Fetch weekly sales data between start_date and end_date.
    """
    query = f"""
    SELECT date, SUM(sales_amount) as total_sales, COUNT(*) as total_transactions
    FROM sales
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY date
    ORDER BY date;
    """
    return pd.read_sql(query, engine)

def get_monthly_statistics(month):
    """
    Fetch aggregated sales data for the month.
    """
    query = f"""
    SELECT product_category, SUM(sales_amount) as total_sales, COUNT(*) as total_transactions
    FROM sales
    WHERE date BETWEEN '{month}-01' AND '{month}-31'
    GROUP BY product_category
    ORDER BY total_sales DESC;
    """
    return pd.read_sql(query, engine)

if __name__ == "__main__":
    # Example of serving data for a single day
    current_day = datetime(2023, 12, 1)
    while current_day.month == 12:
        print(f"Serving data for {current_day.strftime('%Y-%m-%d')}")
        daily_data = get_daily_data(current_day.strftime('%Y-%m-%d'))
        print(daily_data.head())  # Debug
        time.sleep(10)
        current_day += timedelta(days=1)
