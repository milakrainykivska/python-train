#The goal of first task is loading weather data from open API to MS SQL Database
# 1 - create table in MS SQL using script
"""
CREATE TABLE weather_data (
    datetime DATETIME,
    temperature FLOAT,
    wind_speed FLOAT,
    precipitation FLOAT
);
"""
# 2 - create Synapse Notebook with next script:
import requests
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
import urllib
import pyodbc

# Configurable parameters
API_BASE_URL = 'https://api.meteomatics.com'
USERNAME = 'gridd_krainykivska_mila'  # better to save in secret key
PASSWORD = 'F8hBaz16SF'  # better to save in secret key
SQL_SERVER = 'MSSQLServer'
SQL_DATABASE = 'MSSQLDatabase'
TABLE_NAME = 'weather_data'

# Weather API parameters
yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
PARAMETERS = "t_2m:C,wind_speed_10m:ms,precip_1h:mm"
LOCATION = "50.4501,30.5234"  # Kyiv coordinates (latitude, longitude)
URL = f"{API_BASE_URL}/{yesterday}/{PARAMETERS}/{LOCATION}/json?model=mix"

def fetch_weather_data(url, username, password):
    """Fetches weather data from the Meteomatics API."""
    response = requests.get(url, auth=(username, password))
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()
    weather_data = {
        "datetime": data["data"][0]["coordinates"][0]["dates"][0]["date"],
        "temperature": data["data"][0]["coordinates"][0]["dates"][0]["value"],
        "wind_speed": data["data"][1]["coordinates"][0]["dates"][0]["value"],
        "precipitation": data["data"][2]["coordinates"][0]["dates"][0]["value"]
    }

    return pd.DataFrame([weather_data])

def write_df_to_sql(df, sql_server, sql_database, table_name):
    """Writes a DataFrame to an MS SQL table using pyodbc."""
    connection_string = (
        f'Driver={{ODBC Driver 17 for SQL Server}};'
        f'Server={sql_server};'
        f'Database={sql_database};'
        'Trusted_Connection=yes;'
    )
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Create table if it doesn't exist
    cursor.execute(f"""
        IF OBJECT_ID('{table_name}', 'U') IS NULL
        CREATE TABLE {table_name} (
            datetime DATETIME,
            temperature FLOAT,
            wind_speed FLOAT,
            precipitation FLOAT
        )
    """)
    connection.commit()

    # Insert DataFrame into the table
    for index, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (datetime, temperature, wind_speed, precipitation)
            VALUES (?, ?, ?, ?)
        """, row['datetime'], row['temperature'], row['wind_speed'], row['precipitation'])
    connection.commit()

    cursor.close()
    connection.close()

def main():
    try:
        df = fetch_weather_data(URL, USERNAME, PASSWORD)
        print(df)
        write_df_to_sql(df, SQL_SERVER, SQL_DATABASE, TABLE_NAME)
        print(f"Data loaded into {TABLE_NAME} table in {SQL_DATABASE} database successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

# 3 - We have to create a pipeline on Synapse with Daily trigger for exaple at 9:00 am or loading daily weather data and have up to date data all the time for futher analytics.
