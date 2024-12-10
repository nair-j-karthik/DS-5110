from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from generate_sales_data import generate_sales_data
from data_preparation import data_prepare

with DAG(
    dag_id='sales_data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
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
            'filename': '/opt/airflow/processed_data/2018-01-01.csv',
        },
    )

    # Define task dependencies
    prepare_sales_data >> stream_sales_data
