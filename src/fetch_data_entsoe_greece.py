import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load API key
load_dotenv()
API_KEY = os.getenv('ENTSOE_API_KEY')

# ENTSO-E API URL
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# Greece bidding zone
in_domain = "10YGR-HTSO-----Y"

# PsrType Mapping
PSR_TYPE_MAP = {
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir"
}

# Define start & end years
start_year = 2022
end_year = 2022

# Function to fetch one day of data
def fetch_one_day(year, month, day, psr_type):
    start = datetime(year, month, day, 0, 0)
    end = start + timedelta(hours=23)

    params = {
        "securityToken": API_KEY,
        "documentType": "A73",
        "processType": "A16",
        "in_Domain": in_domain,
        "periodStart": start.strftime("%Y%m%d%H%M"),
        "periodEnd": end.strftime("%Y%m%d%H%M"),
        "PsrType": psr_type
    }
    
    print(f"📡 Fetching {year}-{month:02d}-{day:02d} for {PSR_TYPE_MAP[psr_type]}...")
    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        print(f"❌ ERROR {response.status_code} - {response.text}")
        return None

    return response.text

# Function to parse XML and extract time-series data
def parse_timeseries_from_xml(xml_data):
    root = ET.fromstring(xml_data)
    namespace = root.tag.split('}')[0].strip('{')
    ns = {'ns': namespace}

    timeseries_data = []

    for timeseries in root.findall('ns:TimeSeries', ns):
        registered_resource = timeseries.find('ns:registeredResource.mRID', ns).text

        # Find plant name if available
        name_elem = timeseries.find('ns:MktPSRType/ns:PowerSystemResources/ns:name', ns)
        plant_name = name_elem.text if name_elem is not None else "Unknown"

        # PsrType
        raw_psr_type = timeseries.find('ns:MktPSRType/ns:psrType', ns).text
        psr_type = PSR_TYPE_MAP.get(raw_psr_type, raw_psr_type)

        for period in timeseries.findall('ns:Period', ns):
            start_time = period.find('ns:timeInterval/ns:start', ns).text
            end_time = period.find('ns:timeInterval/ns:end', ns).text

            for point in period.findall('ns:Point', ns):
                position = int(point.find('ns:position', ns).text)
                generation = float(point.find('ns:quantity', ns).text)
                
                timeseries_data.append({
                    'plant_name': plant_name,  
                    'psr_type': psr_type,
                    'start_time': start_time,
                    'end_time': end_time,
                    'position': position,
                    'Generation (MW)': generation
                })
    
    return pd.DataFrame(timeseries_data)

# Main process — Fetch data for all years and save
os.makedirs('data', exist_ok=True)

for year in range(start_year, end_year + 1):
    all_timeseries = pd.DataFrame()

    for month in range(1, 13):
        for day in range(1, 32):
            try:
                for psr_type in PSR_TYPE_MAP.keys():
                    xml_data = fetch_one_day(year, month, day, psr_type)
                    if xml_data:
                        timeseries_df = parse_timeseries_from_xml(xml_data)
                        all_timeseries = pd.concat([all_timeseries, timeseries_df])
            except ValueError:
                continue  # Skip invalid days (e.g., Feb 30)

    # Save yearly data
    year_file = f"data/timeseries_{year}.csv"
    all_timeseries.to_csv(year_file, index=False)
    print(f"✅ {year_file} saved!")

print("🚀 Done fetching all hydro time-series data!")


