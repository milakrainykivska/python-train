#The goal of first task is loading weather data from open API to MS SQL Database

import requests
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
import urllib
import pyodbc
from azure.keyvault.secrets import SecretClient
from sqlalchemy import create_engine

# Configurable parameters
API_BASE_URL = 'https://api.meteomatics.com'
SQL_SERVER = 'MSSQLServer'
SQL_DATABASE = 'MSSQLDatabase'
TABLE_NAME = 'weather_data'

# Fetch secrets from Key Vault
KEY_VAULT_NAME = "my-key-vault"
KV_URI = f"https://{KEY_VAULT_NAME}.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KV_URI, credential=credential)
USERNAME = client.get_secret("MeteomaticsUsername").value
PASSWORD = client.get_secret("MeteomaticsPassword").value

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

def create_sql_engine(sql_server, sql_database):
    """Creates and returns a SQLAlchemy engine."""
    connection_string = (
        f"mssql+pyodbc:///?odbc_connect="
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={sql_server};"
        f"Database={sql_database};"
        f"Trusted_Connection=yes;"
    )
    return create_engine(connection_string)

def create_table_if_not_exists(engine, table_name):
    """Creates the table if it doesn't exist in the database."""
    create_table_query = f"""
        IF OBJECT_ID('{table_name}', 'U') IS NULL
        CREATE TABLE {table_name} (
            datetime DATETIME,
            temperature FLOAT,
            wind_speed FLOAT,
            precipitation FLOAT
        )
    """
    with engine.connect() as connection:
        connection.execute(create_table_query)

def write_df_to_sql(df, engine, table_name):
    """Writes a DataFrame to an MS SQL table using SQLAlchemy."""
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Data successfully written to {table_name} table.")
    except SQLAlchemyError as e:
        print(f"Error writing data to SQL: {e}")

def main():
    df = fetch_weather_data(URL, USERNAME, PASSWORD)
    if not df.empty:
        engine = create_sql_engine(SQL_SERVER, SQL_DATABASE)
        create_table_if_not_exists(engine, TABLE_NAME)
        write_df_to_sql(df, engine, TABLE_NAME)
    else:
        print("No data fetched, skipping database write.")

if __name__ == "__main__":
    main()
