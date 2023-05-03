"""
Author: DieselTech
URL: https://github.com/DieselTech/Kavita-API-Scripts
Date created: May 5, 2023, 16:00

Description:
This will queue up all the libraries on your kavita install to scan. Since a general "Scan all libraries" doesn't exist within the server, this will tell it to scan each library one by one. If you have a lot of libraries added
this can take some time. According to the server logs there could also be a 3 hour delay before a scan starts. 

Software requirements:
- Python 3 or later
- requests
- json
- urlparse

Usage:
python scan_all_libraries.py
"""

import os
import requests
from urllib.parse import urlparse

path = "M:\comics"  # Replace with the path to your comics || This is for if your running the script on a different machine than your kavita host. 
exclude_list = ["Marvel", "DC Comics", "Image"]  # Replace with your publisher names to exclude. Thsese are generally the biggest folders in the library and will take a long time to scan. 

# Do NOT change anything below this line

url = input("Paste in your full ODPS URL from your Kavita user dashboard (/preferences#clients): ")

parsed_url = urlparse(url)

host_address = parsed_url.scheme + "://" + parsed_url.netloc
api_key = parsed_url.path.split('/')[-1]

print("Host Address:", host_address)
print("API Key:", api_key)

login_endpoint = "/api/Plugin/authenticate"
library_endpoint = "/api/Library/create"

try:
    apikeylogin = requests.post(host_address + login_endpoint + "?apiKey=" + api_key + "&pluginName=pythonFolderCreateScript")
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

# Line 25 where 'os.path.join' is for accounting for dockers inside container paths. It will put /comics in front of the folder name outputted by the path

for entry in os.scandir(path):
    if entry.is_dir() and entry.name not in exclude_list:
        full_path = os.path.join('/comics/', entry.name)
        payload = {
            "name": entry.name,
            "type": 1, # 0 = manga, 1 = comics, 2 = books
            "folders": [full_path]
         }
        try:
            response = requests.post(host_address + library_endpoint, headers=headers, json=payload)
            response.raise_for_status() # check if the response code indicates an error
            print(f"Folder '{entry.name}' sent. Response: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error creating library for folder '{entry.name}':", e)
