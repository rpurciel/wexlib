import requests
import sys
import os

sys.path.insert(0, '/Users/ryanpurciel/Development/wexlib/src')
sys.path.insert(0, '/Users/rpurciel/Development/wexlib/src') #FOR TESTING ONLY!!!

import wexlib.keys_private as key

DEF_FIELDS = 'sensor_index,pm2.5_atm_a,pm2.5_atm_b'

# def get_sensor_history(key, start, end, fields):

# 	request

sensor_id = 127927

url = f"https://api.purpleair.com/v1/sensors/{sensor_id}/history/csv"

header = {'X-API-Key' : key.API_KEY_PURPLEAIR}
parameters = {
	'fields' : DEF_FIELDS
}
request = requests.get(url, headers=header, params=parameters, stream=True)

file_name = f'{sensor_id}_data.csv'

file_path = os.path.join('/Users/rpurciel/Development/wexlib/tests/temp', file_name)

with open(file_path, 'wb') as fd:
    for chunk in request.iter_content(chunk_size=128):
        fd.write(chunk)