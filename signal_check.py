import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector as db
from sqlalchemy import create_engine
import streamlit as st

# Data Read
df = pd.read_csv(r"C:\Users\B Rashmi Surya Vetri\Desktop\Python\traffic_stops.csv")


# Data Cleaning
# Drop Unwanted Columns
df.drop( ["driver_age_raw", "violation_raw"] , axis = 1, inplace=True)
# Drop fully empty columns
df.dropna(axis=1, how='all', inplace=True)
# Check null columns
df.isnull().sum()
# Change null in search type column
df.fillna({"search_type":"Unknown"}, inplace=True)
# Check rows missing
rows_missing = df[df.isnull().any(axis=1)]
print(rows_missing)
# Check duplicates
duplicates = df[df.duplicated()]
print("Total duplicate rows:",len(duplicates))
duplicates_subset = df[df.duplicated(subset=["vehicle_number"])]
print("Duplicates based on vehicle:",len(duplicates_subset))
df["timestamp"] = pd.to_datetime(df["stop_date"].astype(str) + " " + df["stop_time"].astype(str))

df["stop_date"] = pd.to_datetime(df["stop_date"], errors="coerce").dt.strftime("%Y-%m-%d")
df["stop_time"] = pd.to_datetime(df["stop_time"], format="%H:%M:%S", errors="coerce").dt.strftime("%H:%M:%S")

# Create MySQL connection
connection = db.connect(

                host = "localhost",
                user = "root",
                password = "Rashsur1995*",
                database = "securecheck"
)
cursor = connection.cursor()

# Create Table
cursor.execute("""CREATE TABLE IF NOT EXISTS signal_check (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stop_date Date,
    stop_time Time,
    country_name VARCHAR(50),
    driver_gender VARCHAR(10),
    driver_age INT,
    driver_race VARCHAR(30),
    violation VARCHAR(50),
    search_conducted BOOLEAN,
    search_type VARCHAR(50),
    stop_outcome VARCHAR(50),
    is_arrested BOOLEAN,
    drugs_related_stop BOOLEAN,
    stop_duration VARCHAR(50),
    vehicle_number VARCHAR(30),
    timestamp DateTime
);
""")

# Create connection
engine = create_engine("mysql+pymysql://root:Rashsur1995*@localhost/securecheck")

# Insert Dataframe into MySQL
df.to_sql("signal_check",engine,if_exists="replace",index=False)

# Fetch data from database
def fetch_data(query):
    connection = create_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result)
            return df
        finally:
            connection.close()
    else:
        return pd.DataFrame()
    
# Streamlit
# Streamlit Dashboard Title
st.set_page_config(page_title="Traffic Police Logs Dashboard", layout="wide")

# Streamlit Title
st.title("👮‍♂️Police Traffic Stop Analysis🚓")

# Load Data from SQL
@st.cache_data
def load_data():
    query="SELECT * from signal_check"
    df= pd.read_sql(query, engine)
    return df
data=load_data()

# Show Dataset Preview
st.subheader("📒Dataset Preview")
st.write(data.head())

# Filters
st.subheader("🚦Filter by Country")
country= st.selectbox("Select a country:",data["country_name"].unique())
filtered= data[data["country_name"]==country]
st.write(filtered.head())


# ------------------------
#FORM: Add New Police Log
# ------------------------
st.header("👮‍♂️Add New Police Log & Predict Outcome👮‍♀️")
with st.form("new_log_form"):
    stop_date = st.date_input("Stop Date")
    stop_time = st.time_input("Stop Time")
    county_name = st.text_input("County Name")
    driver_gender = st.selectbox("Driver Gender", ["male", "female", "other"])
    driver_age = st.number_input("Driver Age", min_value=16, max_value=100, step=1)
    driver_race = st.text_input("Driver Race")
    search_conducted = st.selectbox("Was a Search Conducted?", [0,1])
    search_type = st.text_input("Search Type")
    drug_related = st.selectbox("Was it Drug Related?", [0,1])
    stop_duration = st.selectbox("Stop Duration", ["0-15 Min","16-30 Min","30+ Min"])
    vehicle_number = st.text_input("Vehicle Number")

    submitted = st.form_submit_button("Predict Stop Outcome & Violation")
    if submitted:
        # Filter data for prediction
        filtered_data = data[
            (data['driver_gender'] == driver_gender) &
            (data['driver_age'] == driver_age) &
            (data['search_conducted'] == int(search_conducted)) &
            (data['stop_duration'] == stop_duration) &
            (data['drugs_related_stop'] ==int(drug_related))
        ]
        # Predict stop outcome
        if not filtered_data.empty:
            predicted_outcome = filtered_data['stop_outcome'].mode()[0]
            predicted_violation = filtered_data['violation'].mode()[0]
        else:
            predicted_outcome = "warning"
            predicted_violation = "speeding" 
        # Natural Language Summary
        search_text = "A search was conducted" if int(search_conducted) else "No search was conducted"
        drugs_text = " Was drug-related" if int(drug_related) else "was not drug-related"

        st.markdown(f"""
        💈 **Prediction Summary**
           **Predicted violation:** {predicted_violation}
           **Predicted Stop Outcome:** {predicted_outcome}
          
         🔞 A {driver_age} year-old {driver_gender} driver in {county_name} was stopped at {stop_time.strftime('%I:%M %p')} on {stop_date}.
         {search_text}, and the stop {drugs_text}.
         Stop Duration: **{stop_duration}**.
         Vehicle Number: **{vehicle_number}**.
         """)


# --- Dictionary of Queries ---
queries = {
    "Top 10 vehicle_number involved in drug_related_stops": """
        SELECT vehicle_number, COUNT(*) AS stop_count
        FROM signal_check
        WHERE drugs_related_stop = TRUE
        GROUP BY vehicle_number
        ORDER BY stop_count DESC
        LIMIT 10;
    """,
    "Top 10 vehicles were most frequently searched": """
        SELECT vehicle_number, COUNT(*) AS search_count
        FROM signal_check
        WHERE search_conducted = TRUE
        GROUP BY vehicle_number
        ORDER BY search_count DESC
        LIMIT 10;
    """,
    "Driver age group had the highest arrest rate": """
       SELECT 
	CASE
		WHEN driver_age BETWEEN 16 AND 25 THEN '16-25'
		WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
		WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'
		WHEN driver_age BETWEEN 46 AND 60 THEN '46-60'
		ELSE '61+'
	END AS age_group,
    (SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END) * 1.0/ COUNT(*)) * 100 AS arrest_rate
    FROM signal_check
    GROUP BY age_group
    ORDER BY arrest_rate DESC;
    """,
    "Gender distribution of drivers stopped in each country": """
        SELECT
	        country_name,
            driver_gender,
            COUNT(*) AS total_stops
        FROM signal_check
        GROUP BY country_name, driver_gender
        ORDER BY country_name, total_stops DESC;
    """,
    "Race and gender combination has the highest search rate": """
        SELECT
	        driver_race,
            driver_gender,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS search_rate
        FROM signal_check
        GROUP BY driver_race, driver_gender
        ORDER BY search_rate DESC;
    """,
    "Time of day sees the most traffic stops": """
        SELECT
	        CASE
		        WHEN HOUR(stop_time) BETWEEN 5 AND 11 THEN 'MORNING'
                WHEN HOUR(stop_time) BETWEEN 12 AND 16 THEN 'AFTERNOON'
                WHEN HOUR(stop_time) BETWEEN 17 AND 21 THEN 'EVENING'
                ELSE 'NIGHT'
	        END AS time_period,
            COUNT(*) AS total_stops
        FROM signal_check
        GROUP BY time_period
        ORDER BY total_stops DESC;
    """,
    "Average stop duration for different violations": """
        SELECT
	        violation,
            AVG(
		        CASE 
			        WHEN stop_duration = '0-15 min' THEN 7.5
                    WHEN stop_duration = '16-30 min' THEN 23
                    WHEN stop_duration = '31-60 min' THEN 45.5
                    WHEN stop_duration = '1 HOUR+' THEN 60
		        END
	        ) AS avg_stop_duration_minutes
        FROM signal_check
        GROUP BY violation
        ORDER BY avg_stop_duration_minutes DESC;
    """,
    "Are stops during the night more likely to lead to arrests": """
        SELECT
            CASE
                WHEN HOUR(stop_time) BETWEEN 21 AND 23 OR HOUR(stop_time) BETWEEN 0 AND 4
                    THEN 'Night (9 PM - 4 AM)'
                ELSE 'Daytime (5 AM - 8 PM)'
            END AS time_period,
            (SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS arrest_rate
        FROM signal_check
        GROUP BY time_period
        ORDER BY arrest_rate DESC;
    """,
    "Violations are most associated with searches or arrests": """
        SELECT
            violation,
            (SUM(CASE WHEN search_conducted = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS search_rate,
            (SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS arrest_rate
        FROM signal_check
        GROUP BY violation
        ORDER BY search_rate DESC, arrest_rate DESC;
    """,
    "Violations are most common among younger drivers (<25)": """
        SELECT
            violation,
            COUNT(*) AS total_stops,
            (COUNT(*) * 1.0 / SUM(COUNT(*)) OVER()) * 100 AS percentage
        FROM signal_check
        WHERE driver_age < 25
        GROUP BY violation
        ORDER BY total_stops DESC;
    """,
    "Is there a violation that rarely results in search or arrest": """
        SELECT
            violation,
            (SUM(CASE WHEN search_conducted = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS search_rate,
            (SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS arrest_rate
        FROM signal_check
        GROUP BY violation
        ORDER BY search_rate ASC, arrest_rate ASC;
    """,
    "Which countries report the highest rate of drug-related stops": """
        SELECT
            country_name,
            (SUM(CASE WHEN drugs_related_stop = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS drug_related_rate
        FROM signal_check
        GROUP BY country_name
        ORDER BY drug_related_rate DESC;
    """,
    "What is the arrest rate by country and violation": """
        SELECT
            country_name,
            violation,
            (SUM(CASE WHEN is_arrested = 'True' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS arrest_rate
        FROM signal_check
        GROUP BY country_name, violation
        ORDER BY arrest_rate DESC;
    """,
    "Which country has the most stops with search conducted": """
        SELECT
            country_name,
            COUNT(*) AS total_searches
        FROM signal_check
        WHERE search_conducted = 'True'
        GROUP BY country_name
        ORDER BY total_searches DESC;
    """,
     "Yearly Breakdown of Stops and Arrests by Country": """
        SELECT
            country_name,
            yr AS year,
            total_stops,
            total_arrests,
            ROUND(total_arrests * 100.0 / total_stops, 2) AS arrest_rate,
            SUM(total_stops) OVER (PARTITION BY country_name ORDER BY yr) AS cumulative_stops,
            SUM(total_arrests) OVER (PARTITION BY country_name ORDER BY yr) AS cumulative_arrests
        FROM (
            SELECT
                country_name,
                YEAR(STR_TO_DATE(stop_date, '%%Y-%%m-%%d')) AS yr,
                COUNT(*) AS total_stops,
                SUM(CASE WHEN is_arrested IN ('True','true',1,'1') THEN 1 ELSE 0 END) AS total_arrests
            FROM signal_check
            GROUP BY country_name, YEAR(STR_TO_DATE(stop_date, '%%Y-%%m-%%d'))
        ) AS yearly_stats
        ORDER BY country_name, yr;
    """,
    "Driver Violation Trends Based on Age and Race": """
        SELECT
            v.driver_race,
            v.age_group,
            v.violation,
            v.total_violations,
            ROUND(v.total_violations * 100.0 / t.race_age_total, 2) AS violation_percentage
        FROM (
            SELECT
                driver_race,
                CASE
                    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'
                    WHEN driver_age BETWEEN 46 AND 55 THEN '46-55'
                    WHEN driver_age BETWEEN 56 AND 65 THEN '56-65'
                    ELSE '65+'
                END AS age_group,
                violation,
                COUNT(*) AS total_violations
            FROM signal_check
            GROUP BY driver_race, age_group, violation
        ) AS v
        JOIN (
            SELECT
                driver_race,
                CASE
                    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'
                    WHEN driver_age BETWEEN 46 AND 55 THEN '46-55'
                    WHEN driver_age BETWEEN 56 AND 65 THEN '56-65'
                    ELSE '65+'
                END AS age_group,
                COUNT(*) AS race_age_total
            FROM signal_check
            GROUP BY driver_race, age_group
        ) AS t
        ON v.driver_race = t.driver_race AND v.age_group = t.age_group
        ORDER BY v.driver_race, v.age_group, violation_percentage DESC;
    """,
    "Time Period Analysis of Stops, Number of Stops by Year,Month, Hour of the Day": """
        SELECT
            YEAR(STR_TO_DATE(stop_date, '%%Y-%%m-%%d')) AS year,
            MONTH(STR_TO_DATE(stop_date, '%%Y-%%m-%%d')) AS month,
            HOUR(STR_TO_DATE(stop_time, '%%H:%%i:%%s')) AS hour_of_day,
            COUNT(*) AS total_stops
        FROM signal_check
        GROUP BY
            YEAR(STR_TO_DATE(stop_date, '%%Y-%%m-%%d')),
            MONTH(STR_TO_DATE(stop_date, '%%Y-%%m-%%d')),
            HOUR(STR_TO_DATE(stop_time, '%%H:%%i:%%s'))
        ORDER BY year, month, hour_of_day;
    """,
    "Violations with High Search and Arrest Rates": """
        SELECT 
            violation,
            ROUND(SUM(CASE WHEN search_conducted IN ('True','true',1) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate,
            ROUND(SUM(CASE WHEN is_arrested IN ('True','true',1) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate,
            RANK() OVER (ORDER BY
                (SUM(CASE WHEN search_conducted IN ('True','true',1) THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) DESC,
                (SUM(CASE WHEN is_arrested IN ('True','true',1) THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) DESC
                ) AS rank_order
        FROM signal_check
        GROUP BY violation
        ORDER BY rank_order;
    """,
    "Driver Demographics by Country": """
        SELECT
            country_name,
            driver_gender,
            driver_race,
            CASE
                WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
                WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
                WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'
                WHEN driver_age BETWEEN 46 AND 55 THEN '46-55'
                WHEN driver_age BETWEEN 56 AND 65 THEN '56-65'
                ELSE '65+'
            END AS age_group,
            COUNT(*) AS total_drivers,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY country_name), 2) AS percentage_in_country
        FROM signal_check
        GROUP BY
            country_name, driver_gender, driver_race, age_group
        ORDER BY country_name, total_drivers DESC;
    """,
    "Top 5 Violations with Highest Arrest Rates": """
        SELECT
            violation,
            ROUND(SUM(CASE WHEN is_arrested IN ('True','true',1) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate
        FROM signal_check
        GROUP BY violation
        ORDER BY arrest_rate DESC
        LIMIT 5;
    """
}    


# --- Streamlit UI ---
st.title("📊 Advance Insights")

# Dropdown menu for queries
selected_query = st.selectbox("Select a Query to Run", list(queries.keys()))

# Run button
if st.button("Run Query"):
    query = queries[selected_query]
    df = pd.read_sql(query, engine)

    # Show Data
    st.subheader("📑 Query Result")
    st.dataframe(df)








