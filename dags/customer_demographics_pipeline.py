from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from data_preparation import data_prepare
from ingest_customer_demographics import generate_customer_demographics_data

with DAG(
    dag_id='customer_demographics_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
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

    # Define task dependencies
    prepare_demographics_data >> ingest_demographics_data
