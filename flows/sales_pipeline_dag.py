from prefect import flow, task
from scripts.data_preparation import data_prepare
from scripts.generate_sales_data import generate_data

@task
def prepare_data_task():
    return data_prepare()

@task
def generate_data_task():
    return generate_data()

@flow
def sales_pipeline_flow():
    prepared_data = prepare_data_task()
    generate_data_task(wait_for=[prepared_data])

if __name__ == "__main__":
    sales_pipeline_flow()
