from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from data_preparation import data_prepare
from ingest_locations import generate_locations_data

with DAG(
    dag_id='locations_data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
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
    prepare_locations_data >> ingest_locations_data
