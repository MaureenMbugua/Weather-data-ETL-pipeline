import requests

API_KEY = '18890a1d8cfd1b3130af9ac578d2c1a5'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

CITIES = ['Nairobi', 'London', 'Tokyo', 'Delhi', 'New York']

for city in CITIES:
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        temp = data['main']['temp']
        weather = data['weather'][0]['description']
        humidity = data['main']['humidity']
        #wind_speed = data['feels']['like']
        
        print(f"\nWeather in {city}")
        print(f"-------------------")
        print(f"Temperature: {temp}°C")
        print(f"Condition: {weather}")
        print(f"Humidity: {humidity}%")
        #print(f"Wind Speed: {wind_speed} m/s")
    else:
        print(f"{city}: ❌ Failed - Status Code {response.status_code}")