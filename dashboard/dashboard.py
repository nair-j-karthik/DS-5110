import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
from streamlit_folium import folium_static
from sqlalchemy import create_engine

# Get the database URI from environment variables
import os
DATABASE_URI = os.getenv("DATABASE_URI", "postgresql://postgres:your_password@localhost:5432/sales_data")

# Connect to the database
@st.cache_resource
def get_connection():
    engine = create_engine(DATABASE_URI)
    return engine

# Load data from PostgreSQL
@st.cache_data
def load_data():
    engine = get_connection()

    # Load datasets from the database
    customer_demographics = pd.read_sql("SELECT * FROM customer_demographics", engine)
    holidays = pd.read_sql("SELECT * FROM holidays", engine)
    locations = pd.read_sql("SELECT * FROM locations", engine)
    sales_data = pd.read_sql("SELECT * FROM sales", engine)

    # Ensure 'date' column is datetime
    sales_data["date"] = pd.to_datetime(sales_data["date"], errors="coerce")

    # Merge sales_data with locations to add city information
    sales_data = sales_data.merge(
        locations[["location_id", "location", "latitude", "longitude"]],
        left_on="location_id",
        right_on="location_id",
        how="left"
    )

    # Merge sales_data with holidays
    holidays["date"] = pd.to_datetime(holidays["date"], errors="coerce")
    sales_data = sales_data.merge(holidays, on="date", how="left")

    return customer_demographics, holidays, locations, sales_data

# Load datasets
customer_demographics, holidays, locations, sales_data = load_data()

# Streamlit app
st.title("Sales Performance Dashboard")

# Sidebar Filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", value=sales_data["date"].min())
end_date = st.sidebar.date_input("End Date", value=sales_data["date"].max())
location_filter = st.sidebar.selectbox("Select Location", ["All"] + list(locations["location"].unique()))
holiday_filter = st.sidebar.selectbox("Select Holiday", ["All"] + list(holidays["holiday_name"].dropna().unique()))

# Filter sales data
filtered_data = sales_data.copy()

if start_date:
    filtered_data = filtered_data[filtered_data["date"] >= pd.Timestamp(start_date)]
if end_date:
    filtered_data = filtered_data[filtered_data["date"] <= pd.Timestamp(end_date)]
if location_filter != "All":
    filtered_data = filtered_data[filtered_data["location"] == location_filter]
if holiday_filter != "All":
    filtered_data = filtered_data[filtered_data["holiday_name"] == holiday_filter]

# Display Key Metrics
st.header("Key Metrics")
if not filtered_data.empty:
    total_customers = filtered_data["customer_id"].nunique()
    total_sales = filtered_data["sales_amount"].sum()
    most_purchased_product = filtered_data["product_name"].mode().iloc[0]

    # Custom layout with larger text and icons
    st.markdown("""
        <style>
        .metric-container {
            display: flex;
            justify-content: space-around;
            margin-bottom: 2rem;
        }
        .metric {
            text-align: center;
            font-size: 1.2rem;
            padding: 1.5rem;
            background-color: #1E1E1E;
            color: white;
            border-radius: 8px;
            width: 30%;
            box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.3);
        }
        .metric h2 {
            margin: 0;
            font-size: 1.8rem;
        }
        .metric p {
            font-size: 1.5rem;
            margin: 0;
        }
        .icon {
            font-size: 2rem;
            margin-bottom: 0.5rem;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div class="metric-container">
            <div class="metric">
                <div class="icon">👥</div>
                <h2>Total Customers</h2>
                <p>{total_customers:,}</p>
            </div>
            <div class="metric">
                <div class="icon">💰</div>
                <h2>Total Sales</h2>
                <p>${total_sales:,.2f}</p>
            </div>
            <div class="metric">
                <div class="icon">📚</div>
                <h2>Most Purchased Product</h2>
                <p>{most_purchased_product}</p>
            </div>
        </div>
    """, unsafe_allow_html=True)
else:
    st.write("No data available for the selected filters.")

# Charts Section
st.header("Charts")
if not filtered_data.empty:
    col1, col2 = st.columns(2)

    # Sales over time (aggregate by month)
    filtered_data["month"] = filtered_data["date"].dt.to_period("M")
    sales_over_time = filtered_data.groupby("month")["sales_amount"].sum()
    with col1:
        st.subheader("Sales Over Time")
        fig1, ax1 = plt.subplots()
        ax1.plot(sales_over_time.index.to_timestamp(), sales_over_time.values, marker="o")
        ax1.set_title("Sales Over Time")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Sales Amount")
        st.pyplot(fig1)

    # Sales by location (top 10 cities)
    sales_by_location = filtered_data.groupby("location")["sales_amount"].sum().nlargest(10)
    with col2:
        st.subheader("Sales by Location")
        fig2, ax2 = plt.subplots()
        ax2.barh(sales_by_location.index, sales_by_location.values)
        ax2.set_title("Top 10 Sales by Location")
        ax2.set_xlabel("Sales Amount")
        ax2.set_ylabel("City")
        st.pyplot(fig2)

# Heatmap Section
st.header("Heatmap of Sales by Location")
if not filtered_data.empty:
    st.subheader("Sales Heatmap")
    heatmap_data = filtered_data.groupby(["latitude", "longitude"])["sales_amount"].sum().reset_index()
    heatmap_map = folium.Map(location=[39.8283, -98.5795], zoom_start=4)  # Center of the USA
    HeatMap(data=heatmap_data[["latitude", "longitude", "sales_amount"]].values.tolist()).add_to(heatmap_map)
    folium_static(heatmap_map)
else:
    st.write("No data available for the heatmap.")

# Comparison Section
st.header("Comparison of Holiday vs. Daily Average Sales")
if not filtered_data.empty and holiday_filter != "All":
    daily_average_sales = filtered_data.groupby("date")["sales_amount"].sum().mean()
    holiday_sales = filtered_data[filtered_data["holiday_name"] == holiday_filter]["sales_amount"].sum()

    st.markdown(f"""
        <p style="font-size: 1.2rem;">
            Average daily sales: <strong>${daily_average_sales:,.2f}</strong><br>
            Sales on <strong>{holiday_filter}</strong>: <strong>${holiday_sales:,.2f}</strong>
        </p>
    """, unsafe_allow_html=True)
else:
    st.write("Select a holiday to see the comparison.")
