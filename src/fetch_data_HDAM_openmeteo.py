import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import os

data_dir = "../data"  
os.makedirs(data_dir, exist_ok=True)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 38.7415,
    "longitude": 21.3642,
    "start_date": "2022-01-01",
    "end_date": "2024-12-31",
    "hourly": ["rain", "precipitation", "precipitation_probability", "apparent_temperature", "temperature_2m",
                "evapotranspiration", "soil_temperature_0cm", "showers", "snowfall", "snow_depth"],
    "timezone": "auto"
}

# Fetch the weather data
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_rain = hourly.Variables(0).ValuesAsNumpy()
hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
hourly_precipitation_probability = hourly.Variables(2).ValuesAsNumpy()
hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
hourly_temperature_2m = hourly.Variables(4).ValuesAsNumpy()
hourly_evapotranspiration = hourly.Variables(5).ValuesAsNumpy()
hourly_soil_temperature_0cm = hourly.Variables(6).ValuesAsNumpy()
hourly_showers = hourly.Variables(7).ValuesAsNumpy()
hourly_snowfall = hourly.Variables(8).ValuesAsNumpy()
hourly_snow_depth = hourly.Variables(9).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["rain"] = hourly_rain
hourly_data["precipitation"] = hourly_precipitation
hourly_data["precipitation_probability"] = hourly_precipitation_probability
hourly_data["apparent_temperature"] = hourly_apparent_temperature
hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["evapotranspiration"] = hourly_evapotranspiration
hourly_data["soil_temperature_0cm"] = hourly_soil_temperature_0cm
hourly_data["showers"] = hourly_showers
hourly_data["snowfall"] = hourly_snowfall
hourly_data["snow_depth"] = hourly_snow_depth

# Create a DataFrame from the hourly data
hourly_dataframe = pd.DataFrame(data=hourly_data)

# Extract the year from the start_date parameter
year = pd.to_datetime(params["start_date"]).year

# Define file path to save data with the year in the file name
file_path = os.path.join(data_dir, f"weather_HDAM.csv")

# Save the DataFrame to a CSV file
hourly_dataframe.to_csv(file_path, index=False)

print(f"Data saved to {file_path}")


