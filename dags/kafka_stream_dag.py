# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import sys
# sys.path.append("/opt/airflow/scripts")  # Add the path dynamically
# from generate_sales_data import generate_sales_data
# from data_preparation import data_prepare

# with DAG(
#     dag_id='sales_data_pipeline',
#     schedule_interval='@daily',
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
# ) as dag:
#     prepare_data = PythonOperator(
#         task_id='prepare_data',
#         python_callable=data_prepare,
#         op_kwargs={'raw_file': '/opt/airflow/data/Sales_April_2019.csv',
#                    'output_file': '/opt/airflow/processed_data/Processed_Data.csv'},
#     )

#     stream_data = PythonOperator(
#         task_id='stream_sales_data',
#         python_callable=generate_sales_data,
#         op_kwargs={'filename': '/opt/airflow/processed_data/Processed_Data.csv'},
#     )

#     prepare_data >> stream_data
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys

# Add the path dynamically
sys.path.append("/opt/airflow/scripts")

# Import the preparation and ingestion scripts
from generate_sales_data import generate_sales_data
from data_preparation import data_prepare
from ingest_customer_demographics import generate_customer_demographics_data
from ingest_locations import generate_locations_data

with DAG(
    dag_id='data_pipeline_all_tables',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # Sales Table Pipeline
    prepare_sales_data = PythonOperator(
        task_id='prepare_sales_data',
        python_callable=data_prepare,
        op_kwargs={
            'raw_file': '/opt/airflow/data/Sales_April_2019.csv',
            'output_file': '/opt/airflow/processed_data/Processed_Data.csv',
        },
    )

    stream_sales_data = PythonOperator(
        task_id='stream_sales_data',
        python_callable=generate_sales_data,
        op_kwargs={
            'filename': '/opt/airflow/processed_data/Data_Source/2018-01-01.csv',
        },
    )

    # Customer Demographics Pipeline
    prepare_demographics_data = PythonOperator(
        task_id='prepare_customer_demographics',
        python_callable=data_prepare,
        op_kwargs={
            'raw_file': '/opt/airflow/data/Sales_April_2019.csv',
            'output_file': '/opt/airflow/processed_data/Processed_Data.csv',
        },
    )

    ingest_demographics_data = PythonOperator(
        task_id='ingest_customer_demographics',
        python_callable=generate_customer_demographics_data,
        op_kwargs={
            'filename': '/opt/airflow/processed_data/customer_demographics.csv',
        },
    )

    # Locations Table Pipeline
    prepare_locations_data = PythonOperator(
        task_id='prepare_locations_data',
        python_callable=data_prepare,
        op_kwargs={
            'raw_file': '/opt/airflow/data/Sales_April_2019.csv',
            'output_file': '/opt/airflow/processed_data/Processed_Data.csv',
        },
    )

    ingest_locations_data = PythonOperator(
        task_id='ingest_locations_data',
        python_callable=generate_locations_data,
        op_kwargs={
            'filename': '/opt/airflow/processed_data/locations.csv',
        },
    )

    # Define task dependencies
    prepare_sales_data >> stream_sales_data
    prepare_demographics_data >> ingest_demographics_data
    prepare_locations_data >> ingest_locations_data
