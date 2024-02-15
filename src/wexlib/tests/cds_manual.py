import requests
import json
import time
import os

from tqdm import tqdm

UID = "199712"
API_KEY = "726b5a1b-a601-4576-a3d8-80d191aa2a70"
BASE_URL = "https://cds.climate.copernicus.eu/api/v2/"

def post_request():

	resource = "reanalysis-era5-pressure-levels"

# 	body_dict = {
#     "cds_user_data": {
#         "product": "ERA5 hourly data on pressure levels from 1940 to present",
#         "geonetworkId": "reanalysis-era5-pressure-levels",
#         "list": [
#             {
#                 "name": "product_type",
#                 "value": "reanalysis"
#             },
#             {
#                 "name": "what",
#                 "value": "why"
#             },
#             {
#                 "name": "variable",
#                 "value": "temperature"
#             },
#             {	
#             	"name": "pressure_level",
#             	"value": "500"
#             },
#             {
#             	"name": "date",
#             	"value": "2023-01-13"
#             },
#             {
#                 "name": "year",
#                 "value": "2023"
#             },
#             {
#                 "name": "month",
#                 "value": "01"
#             },
#             {
#                 "name": "day",
#                 "value": "13"
#             },
#             {
#                 "name": "time",
#                 "value": "20:00"
#             }
#         ]
#     }
# }

	body_dict = {
    "product_type": "reanalysis",
    "variable": "temperature",
    "pressure_level": "500",
    "format": "grib",
    "year": "2023",
    "month": "09",
    "day": "02",
    "time": "06:00"
}

	r = requests.post(f"{BASE_URL}/resources/{resource}", auth=(UID, API_KEY), data=json.dumps(body_dict))
	return r.json()['request_id']

task_id = post_request()
completed = False

while not completed:
	r = requests.get(f"{BASE_URL}/tasks/{task_id}", auth=(UID, API_KEY))
	if r.json()['state'] == 'completed':
		completed = True
		print("Completed")
	else:
		print("Not completed yet")
		time.sleep(15)

target_url = r.json()['location']

d = requests.get(target_url, stream=True)

dest_loc = '/Users/rpurciel/Development/wexlib/src/wexlib/tests'

with open(os.path.join(dest_loc, 'target.grib'), 'wb') as dest_file:
	for chunk in d.iter_content(chunk_size=128):
		dest_file.write(chunk)

print("done")

# with tqdm(desc="Retrying in...", bar_format='{desc} |{bar}|',total=30) as prog:
# 	for i in range(0, 30, 1):
# 		prog.update()
# 		prog.set_description('Retrying in ' + tqdm.format_interval(30-i))
# 		time.sleep(1)



