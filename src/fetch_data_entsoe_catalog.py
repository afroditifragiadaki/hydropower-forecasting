import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from datetime import datetime

# Load API key
load_dotenv()
API_KEY = os.getenv('ENTSOE_API_KEY')

# ENTSO-E API URL
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# One-day discovery fetch
start = datetime(2024, 1, 1, 0, 0)
end = datetime(2024, 1, 2, 0, 0)

# Greece bidding zone
in_domain = "10YGR-HTSO-----Y"

# PsrType Mapping
PSR_TYPE_MAP = {
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir"
}

# Fetch one day of data for a specific PsrType
def fetch_one_day(psr_type):
    params = {
        "securityToken": API_KEY,
        "documentType": "A73",
        "processType": "A16",
        "in_Domain": in_domain,
        "periodStart": start.strftime("%Y%m%d%H%M"),
        "periodEnd": end.strftime("%Y%m%d%H%M"),
        "PsrType": psr_type
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.text

# Parse and extract correct EICs
def parse_plants_from_xml(xml_data):
    root = ET.fromstring(xml_data)
    namespace = root.tag.split('}')[0].strip('{')
    ns = {'ns': namespace}

    plants = []

    for timeseries in root.findall('ns:TimeSeries', ns):
        registered_resource = timeseries.find('ns:registeredResource.mRID', ns).text

        # Find name if available
        name_elem = timeseries.find('ns:MktPSRType/ns:PowerSystemResources/ns:name', ns)
        plant_name = name_elem.text if name_elem is not None else "Unknown"

        # PsrType
        raw_psr_type = timeseries.find('ns:MktPSRType/ns:psrType', ns).text
        psr_type = PSR_TYPE_MAP.get(raw_psr_type, raw_psr_type)

        plants.append({
            'eic_code': registered_resource,
            'plant_name': plant_name,
            'psr_type': psr_type
        })

    return pd.DataFrame(plants).drop_duplicates()

# Main process — fetch all hydro types
all_plants = pd.DataFrame()

for psr_type in PSR_TYPE_MAP.keys():
    xml_data = fetch_one_day(psr_type)
    plants_df = parse_plants_from_xml(xml_data)
    all_plants = pd.concat([all_plants, plants_df])

# Save the corrected list
os.makedirs('data', exist_ok=True)
all_plants.to_csv('data/plant_catalog.csv', index=False)

print("✅ Plant catalog saved to data/plant_catalog.csv")


