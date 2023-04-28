"""
Author: DieselTech
URL: https://github.com/DieselTech/Kavita-API-Scripts
Date created: April 28, 2023, 2:30 PM

Description:
This will queue up all the libraries on your kavita install to scan. Since a general "Scan all libraries" doesn't exist within the server, this will tell it to scan each library one by one. If you have a lot of libraries added
this can take some time. According to the server logs there could also be a 3 hour delay before a scan starts. 

Software requirements:
- Python 3 or later
- requests
- json

Usage:
python scan_all_libraries.py
"""

import requests
import json

url = "http://192.168.0.xxx:5000/api/Library" # Change with your own instances URL
scan_url = "http://192.168.0.xxx:5000/api/Library/scan" # Same as above, but leave the API endpoint 
jwt_token = "YOUR_JWT_TOKEN_HERE"

# Get your own JWT token by going to dev tools in your web browser, opening up the 'Storage' tab and then go to local storage. The token can be tricky to copy and paste since it's crazy long. Use a text editior with wordwrap. 

# Do NOT change anything below this line

headers = {
    "Authorization": f"Bearer {jwt_token}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers) # First, get the list of every library that is registered on your system.  

if response.status_code == 200: # As long as the first API call to get all the data is successful
    data = response.json()      # Store the reults as 'data'
    for item in data:           
        id = item["id"]
        payload = {
            "id": id,
        }
        scan_response = requests.post(scan_url, headers=headers, json=payload) # Submit results to the scan API URL as defined at the top
        if scan_response.status_code == 200:
            print(f"Successfully scanned / queued library number {id}") # 
        else:
            print(f"Failed to scan library item {id}")
else:
    print("Error: Failed to retrieve data from the API.")
