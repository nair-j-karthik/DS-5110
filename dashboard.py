# dashboard.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
 
# Database connection
DATABASE_URI = 'sales_dashboard.db'
 
# Connect to the SQLite database
def connect_db():
    conn = sqlite3.connect(DATABASE_URI)
    return conn
 
# Load data from the database
def load_data(table_name):
    conn = connect_db()
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df
 
# Plot sales trend over time
def plot_sales_trend(sales_df):
    st.subheader("Sales Trend Over Time")
    fig, ax = plt.subplots()
    sales_df.groupby('date')['sales_amount'].sum().plot(ax=ax)
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Sales")
    st.pyplot(fig)
 
# Plot the effect of weather on sales
def plot_weather_impact(sales_df, weather_df):
    st.subheader("Weather Impact on Sales")
    merged_df = pd.merge(sales_df, weather_df, on='date')
    fig, ax = plt.subplots()
    ax.scatter(merged_df['temperature'], merged_df['sales_amount'], alpha=0.5)
    ax.set_xlabel("Temperature")
    ax.set_ylabel("Sales")
    st.pyplot(fig)
 
# Plot Google Trends data
def plot_google_trends(trends_df):
    st.subheader("Google Search Trends")
    fig, ax = plt.subplots()
    trends_df.set_index('date').plot(ax=ax)
    ax.set_ylabel("Search Interest")
    st.pyplot(fig)
 
# Streamlit dashboard layout
if __name__ == "__main__":
    st.title("Customer Behavior and Sales Dashboard")
    
    # Load data from database
    sales_data = load_data('sales')
    weather_data = load_data('weather_data')
    trends_data = load_data('google_trends')
    
    # Display data overview
    st.write("### Sales Data Overview", sales_data.head())
    
    # Plot charts
    plot_sales_trend(sales_data)
    plot_weather_impact(sales_data, weather_data)
    plot_google_trends(trends_data)
    
    st.write("Analyze the impact of weather and search trends on sales performance.")