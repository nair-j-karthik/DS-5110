-- Create the Airflow metadata database
CREATE DATABASE airflow;

-- Grant privileges to the PostgreSQL user
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

-- Create the Superset metadata database
CREATE DATABASE superset_metadata;

-- Grant privileges to the PostgreSQL user
GRANT ALL PRIVILEGES ON DATABASE superset_metadata TO postgres;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE sales_data TO postgres;

-- Connect to the database
\connect sales_data;

-- Create the table for sales data
CREATE TABLE IF NOT EXISTS sales_data (
    sale_id SERIAL PRIMARY KEY,
    product VARCHAR(255),
    quantity_sold INT,
    each_price FLOAT,
    sales FLOAT,
    sale_date TIMESTAMP,
    day INT,
    month INT,
    year INT
);