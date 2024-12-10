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
    data['sales'] = data['quantity_sold'] * data['each_price']
    data['sale_date'] = pd.to_datetime(data['sale_date'])
    data['day'] = data['sale_date'].dt.day
    data['month'] = data['sale_date'].dt.month
    data['year'] = data['sale_date'].dt.year

    # Step 3: Save cleaned data
    data.to_csv(output_file, index=False)
