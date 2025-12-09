# app.py
import os
import time
import requests
import pandas as pd
from datetime import datetime
import mysql.connector
from prefect import task, flow
from mysql.connector import Error


# ----- ENV VARIABLES -----
API_KEY = os.getenv("da6946ee18b144163bfa8f881c96643c", "")
CITY = os.getenv("CITY", "Muscat")
DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rootpassword")
DB_NAME = os.getenv("DB_NAME", "weather_db")


@task
def extract_weather():
    """Fetch weather from API"""
    url = f"https://api.openweathermap.org/data/2.5/weather?q={Tokyo}&appid={da6946ee18b144163bfa8f881c96643c}"
    response = requests.get(url)
    response.raise_for_status()
    print("Weather extracted.")
    return response.json()


@task
def transform_weather(data):
    """Convert to DataFrame"""
    df = pd.DataFrame([{
        "time": datetime.utcnow(),
        "temp_kelvin": data["main"]["temp"],
        "temp_celsius": data["main"]["temp"] - 273.15,
        "humidity": data["main"]["humidity"],
        "condition": data["weather"][0]["main"]
    }])
    print("Weather transformed.")
    return df


@task
def load_to_mysql(df):
    """Insert into MySQL database"""
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True
    )
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        time DATETIME,
        temp_kelvin FLOAT,
        temp_celsius FLOAT,
        humidity INT,
        condition VARCHAR(100)
    );
    """)

    insert_query = """
        INSERT INTO weather_data (time, temp_kelvin, temp_celsius, humidity, condition)
        VALUES (%s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row["time"],
            row["temp_kelvin"],
            row["temp_celsius"],
            row["humidity"],
            row["condition"]
        ))

    cur.close()
    conn.close()
    print("Inserted row into MySQL.")


@flow
def weather_etl_flow():
    """Main Prefect flow"""
    raw = extract_weather()
    df = transform_weather(raw)
    load_to_mysql(df)


if __name__ == "__main__":
    weather_etl_flow()
