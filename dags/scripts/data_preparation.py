import pandas as pd
import os

def data_prepare(raw_file, output_file):
    """
    Prepare and clean sales data for analysis and streaming.
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Step 1: Load raw data
    data = pd.read_csv(raw_file)

    # Step 2: Clean and process data
    data = data.rename(columns={
        'Order ID': 'sale_id',
        'Quantity Ordered': 'quantity_sold',
        'Price Each': 'each_price',
        'Order Date': 'sale_date'
    })

    # Ensure columns are numeric
    data['quantity_sold'] = pd.to_numeric(data['quantity_sold'], errors='coerce')
    data['each_price'] = pd.to_numeric(data['each_price'], errors='coerce')

    # Fill missing or invalid values
    data['quantity_sold'].fillna(0, inplace=True)
    data['each_price'].fillna(0, inplace=True)

    # Calculate sales
    data['sales'] = data['quantity_sold'] * data['each_price']

    # Process date column
    data['sale_date'] = pd.to_datetime(data['sale_date'], errors='coerce')
    data['day'] = data['sale_date'].dt.day
    data['month'] = data['sale_date'].dt.month
    data['year'] = data['sale_date'].dt.year

    # Step 3: Save cleaned data
    data.to_csv(output_file, index=False)
