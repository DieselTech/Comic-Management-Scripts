"""
Author: DieselTech
URL: https://github.com/DieselTech/Kavita-API-Scripts
Date created: May 5, 2023, 21:30 PM

Description:
This will queue up all the libraries on your kavita install to scan. S If you have a lot of libraries added this can take some time. According to the server logs there could also be a 3 hour delay before a scan starts. 

Software requirements:
- Python 3 or later
- requests
- json

Usage:
python scan_all_libraries.py
"""
import requests
import json
from urllib.parse import urlparse

url = input("Paste in your full ODPS URL from your Kavita user dashboard (/preferences#clients): ")

parsed_url = urlparse(url)

host_address = parsed_url.scheme + "://" + parsed_url.netloc
api_key = parsed_url.path.split('/')[-1]

print("Host Address:", host_address)
print("API Key:", api_key)

login_endpoint = "/api/Plugin/authenticate"
library_endpoint = "/api/Library"
scan_endpoint = "/api/Library/scan"
try:
    apikeylogin = requests.post(host_address + login_endpoint + "?apiKey=" + api_key + "&pluginName=pythonScanScript")
    apikeylogin.raise_for_status() # check if the response code indicates an error
    jwt_token = apikeylogin.json()['token']
#    print("JWT Token:", jwt_token) # Only for debug 
except requests.exceptions.RequestException as e:
    print("Error during authentication:", e)
    exit()

headers = {
    "Authorization": f"Bearer {jwt_token}",
    "Content-Type": "application/json"
}
response = requests.get(host_address + library_endpoint, headers=headers)

if response.status_code == 200: # As long as the first API call to get all the data is successful
    data = response.json()      # Store the reults as 'data'
    for item in data:           
        id = item["id"]
        scan_response = requests.post(host_address + scan_endpoint + "?libraryId=" + str(id), headers=headers) # Submit results to the scan API
        if scan_response.status_code == 200:
            print(f"Successfully scanned / queued library number {id}") # 
        else:
            print(f"Failed to scan library item {id}")
            print(scan_response)
else:
    print("Error: Failed to retrieve data from the API.")
