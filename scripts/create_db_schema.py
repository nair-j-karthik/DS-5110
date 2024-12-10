import psycopg2

def create_sales_table():
    # Database connection
    conn = psycopg2.connect(
        host="localhost",
        database="sales_data",
        user="postgres",
        password="your_password"
    )
    cursor = conn.cursor()

    # SQL command to create the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales_data (
        order_id SERIAL PRIMARY KEY,
        product TEXT NOT NULL,
        quantity_ordered FLOAT NOT NULL,
        price_each FLOAT NOT NULL,
        order_date TIMESTAMP NOT NULL,
        purchase_address TEXT NOT NULL
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Table sales_data created or verified successfully.")

    # Close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_sales_table()
