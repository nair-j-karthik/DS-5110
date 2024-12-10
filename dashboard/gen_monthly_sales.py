import os
import pandas as pd

# Define the base directory
BASE_DIR = "./data_source/sales_data"  # Update this path to your folder structure
OUTPUT_DIR = "./monthly_sales"  # Directory to save the monthly aggregated files

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def process_monthly_sales(base_dir, output_dir):
    """
    Process sales data from the folder structure and generate monthly aggregated files.
    """
    # Iterate through each year folder
    for year in os.listdir(base_dir):
        year_path = os.path.join(base_dir, year)
        if os.path.isdir(year_path):  # Ensure it's a directory
            for month in os.listdir(year_path):
                month_path = os.path.join(year_path, month)
                if os.path.isdir(month_path):  # Ensure it's a directory
                    all_files = [
                        os.path.join(month_path, file)
                        for file in os.listdir(month_path)
                        if file.endswith(".csv")
                    ]

                    # Read and concatenate all CSV files for the month
                    monthly_data = pd.concat(
                        [pd.read_csv(file) for file in all_files], ignore_index=True
                    )

                    # Aggregate sales data
                    monthly_summary = monthly_data.groupby("product_category").agg(
                        total_sales=("sales_amount", "sum"),
                        total_transactions=("sales_amount", "count")
                    ).reset_index()

                    # Save the monthly summary
                    output_file = os.path.join(
                        output_dir, f"Sales_{year}_{month}.csv"
                    )
                    monthly_summary.to_csv(output_file, index=False)
                    print(f"Saved: {output_file}")

if __name__ == "__main__":
    process_monthly_sales(BASE_DIR, OUTPUT_DIR)
