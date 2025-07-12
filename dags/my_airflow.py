# Import core Airflow components
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook  # To connect to PostgreSQL
from airflow.decorators import task  # For declaring Python functions as tasks

# Import standard libraries
from datetime import datetime, timedelta
import requests  # To make HTTP requests to external APIs

# List of cities to collect weather data for
CITIES = [
    'Nairobi', 'London', 'Kampala', 'Thika', 'Beijing',
    'New York', 'Paris', 'Tokyo', 'Sydney', 'Moscow',
    'Berlin', 'Madrid', 'Rome', 'Los Angeles', 'Chicago',
    'Toronto', 'Vancouver', 'Dubai', 'Singapore', 'Hong Kong',
    'Bangkok', 'Istanbul', 'Cairo', 'Johannesburg', 'Buenos Aires',
    'Lagos', 'Lima', 'Mumbai', 'Delhi', 'Shanghai',
    'Seoul', 'Mexico City', 'Jakarta', 'Rio de Janeiro', 'Sao Paulo',
    'Karachi', 'Manila', 'Tehran', 'Baghdad', 'Dhaka',
    'Kinshasa', 'Casablanca', 'Algiers', 'Accra'
]

# The Airflow connection ID for PostgreSQL (configured in the Airflow UI or environment)- connect to docker
POSTGRES_CONN_ID = 'postgres_default'

# The base URL for the OpenWeatherMap API
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

# Function to read the API key from a file
def load_api_key():
    with open('/usr/local/airflow/dags/credentials.txt', 'r') as file:
        return file.read().strip()

API_KEY = load_api_key()

# Default parameters for the DAG (like retry behavior, owner, etc.) - how we want airflow to run
#things can go wrong when trying to fetch data from API - retry toc onfirm
default_args = {
    'owner': 'airflow',  # The owner of the DAG
    'start_date': datetime(2025, 6, 21),  # When the DAG should start running
    'retries': 3,  # Number of times to retry on failure
    'retry_delay': timedelta(minutes=5)  # Wait time between retries
}

# Define the DAG
with DAG(
    dag_id='weather_etl_pipeline',  # Unique name for the DAG
    default_args=default_args,
    schedule='@daily',  # Run once per day
    catchup=False  # Do not backfill for previous dates
) as dag:

    # Task: Extract weather data for each city
    @task()
    def extract_weather_data():
        all_data = []
        for city in CITIES:
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'  # Celsius
            }
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                all_data.append((city, data))
            else:
                # If the API call fails, raise an error to fail the task
                raise Exception(f"Failed to fetch data for {city}. Status code: {response.status_code}")
        return all_data  # Returns a list of (city, data) tuples

    # Task: Clean and format the weather data
    @task()
    def transform_weather_data(city_data_list):
        transformed_data = []
        for city, data in city_data_list:
            main = data['main']
            wind = data['wind']
            sys = data['sys']
            weather_code = data['weather'][0]['id']
            now = datetime.now()

            # Create a clean Python dictionary for each city’s weather
            transformed_data.append({
                'city': city,
                'temperature': main['temp'],
                'feelsLike': main['feels_like'],
                'minimumTemp': main['temp_min'],
                'maximumTemp': main['temp_max'],
                'pressure': main['pressure'],
                'humidity': main['humidity'],
                'windspeed': wind['speed'],
                'winddirection': wind.get('deg', 0),  # Default to 0 if missing
                'weathercode': weather_code,
                'timeRecorded': now,
                'sunrise': datetime.fromtimestamp(sys['sunrise']),
                'sunset': datetime.fromtimestamp(sys['sunset'])
            })
        return transformed_data

    # Task: Load data into PostgreSQL using PostgresHook
    @task()
    def load_weather_data(data_list):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create a table if it doesn’t exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(255) PRIMARY KEY,
            temperature FLOAT,
            feelsLike FLOAT,
            minimumTemp FLOAT,
            maximumTemp FLOAT,
            pressure INT,
            humidity INT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timeRecorded TIMESTAMP,
            sunrise TIMESTAMP,
            sunset TIMESTAMP
        );
        """)

        # Insert or update records using UPSERT (Postgres ON CONFLICT)
        for data in data_list:
            cursor.execute("""
            INSERT INTO weather_data (
                city, temperature, feelsLike, minimumTemp, maximumTemp,
                pressure, humidity, windspeed, winddirection, weathercode,
                timeRecorded, sunrise, sunset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                feelsLike = EXCLUDED.feelsLike,
                minimumTemp = EXCLUDED.minimumTemp,
                maximumTemp = EXCLUDED.maximumTemp,
                pressure = EXCLUDED.pressure,
                humidity = EXCLUDED.humidity,
                windspeed = EXCLUDED.windspeed,
                winddirection = EXCLUDED.winddirection,
                weathercode = EXCLUDED.weathercode,
                timeRecorded = EXCLUDED.timeRecorded,
                sunrise = EXCLUDED.sunrise,
                sunset = EXCLUDED.sunset;
            """, (
                data['city'], data['temperature'], data['feelsLike'],
                data['minimumTemp'], data['maximumTemp'], data['pressure'],
                data['humidity'], data['windspeed'], data['winddirection'],
                data['weathercode'], data['timeRecorded'], data['sunrise'],
                data['sunset']
            ))

        # Finalize the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()

    # Set the order of execution: extract → transform → load
    extracted = extract_weather_data()
    transformed = transform_weather_data(extracted)
    load_weather_data(transformed)