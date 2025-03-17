import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import os

# Ensure the data directory exists
data_dir = "../data"  # Adjust this based on your folder structure
os.makedirs(data_dir, exist_ok=True)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "start_date": "2022-01-01",
    "end_date": "2022-01-02",
    "hourly": ["soil_temperature_7_to_28cm", "soil_temperature_0_to_7cm", "precipitation", "rain"],
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
hourly_soil_temperature_7_to_28cm = hourly.Variables(0).ValuesAsNumpy()
hourly_soil_temperature_0_to_7cm = hourly.Variables(1).ValuesAsNumpy()
hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
hourly_rain = hourly.Variables(3).ValuesAsNumpy()

hourly_data = {
    "date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
}

# Add other variables to the hourly data dictionary
hourly_data["soil_temperature_7_to_28cm"] = hourly_soil_temperature_7_to_28cm
hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
hourly_data["precipitation"] = hourly_precipitation
hourly_data["rain"] = hourly_rain

# Create a DataFrame from the hourly data
hourly_dataframe = pd.DataFrame(data=hourly_data)

# Define file path to save data
file_path = os.path.join(data_dir, "weather_timeseries_2022.csv")

# Save the DataFrame to a CSV file
hourly_dataframe.to_csv(file_path, index=False)
print(f"✅ Data saved to {file_path}")

