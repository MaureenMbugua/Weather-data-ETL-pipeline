# Import necessary Python modules
import json                  # To work with JSON data from the API
import csv                   # To write data into a CSV file
from datetime import datetime  # To handle date and time formatting
import requests              # To make HTTP requests to the weather API

# List of cities to collect weather data for
# You can add or remove cities as needed

city_names = [
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


# This is the base URL for OpenWeather API.
# We'll add the city name and API key to it later.

base_url = 'https://api.openweathermap.org/data/2.5/weather?q='

# Read your API key from a local file.
# Make sure the file "credentials.txt" exists and contains only the API key.

with open("credentials.txt", 'r') as f:
    api_key = f.read().strip()  # Removes any extra whitespace or new lines

# Function to convert temperature from Kelvin (API default) to Celsius.

def kelvin_to_celsius(temp_k):
    return temp_k - 273.15


# Function to fetch and transform weather data for one city.
# Returns a dictionary with clean and structured data if successful,
# otherwise prints the error and returns None.

def etl_weather_data(city):
    # Build the complete API URL by adding city and API key
    url = base_url + city + "&APPID=" + api_key

    # Send a GET request to the weather API
    response = requests.get(url)

    # Convert the response to a dictionary
    data = response.json()

    # If the request was successful (HTTP status code 200)
    if response.status_code == 200:
        return {
            "city": data["name"],  # City name
            "description": data["weather"][0]['description'],  # Weather condition
            "temperature": kelvin_to_celsius(data["main"]["temp"]),  # Current temperature
            "feelsLike": kelvin_to_celsius(data["main"]["feels_like"]),  # Feels-like temperature
            "minimumTemp": kelvin_to_celsius(data["main"]["temp_min"]),  # Minimum temperature
            "maximumTemp": kelvin_to_celsius(data["main"]["temp_max"]),  # Maximum temperature
            "pressure": data["main"]["pressure"],  # Atmospheric pressure
            "humidity": data["main"]["humidity"],  # Humidity %
            "windSpeed": data["wind"]["speed"],  # Wind speed in m/s
            "timeRecorded": datetime.utcfromtimestamp(data['dt'] + data['timezone']).isoformat(),  # Timestamp
            "sunrise": datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']).isoformat(),  # Sunrise time
            "sunset": datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']).isoformat()  # Sunset time
        }
    else:
        # If API call fails, print the error message
        print(f"Failed to fetch data for {city}. Error: {data.get('message')}")
        return None


# Main function that coordinates the workflow
# - Creates the CSV file
# - Fetches weather for each city
# - Writes each result into the CSV

def main():
    csv_file = "weather_data.csv"  # Output file name

    # Open CSV file once and write the header row
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "city", "description", "temperature", "feelsLike", "minimumTemp",
            "maximumTemp", "pressure", "humidity", "windSpeed",
            "timeRecorded", "sunrise", "sunset"
        ])
        writer.writeheader()  # Write column names at the top of the file

        # Loop through all the cities one by one
        for city in city_names:
            print(f"Fetching data for {city}...")  # Let user know the process has started
            weather_data = etl_weather_data(city)  # Get the weather data

            # If data was fetched successfully, write it to the file
            if weather_data:
                writer.writerow(weather_data)  # Add the row to CSV
                print(f"✔️ Saved weather for {city}")
            else:
                print(f"❌ Could not fetch data for {city}")


# This block ensures that main() runs only
# when the script is run directly (not imported as a module)

if __name__ == "__main__":
    main()