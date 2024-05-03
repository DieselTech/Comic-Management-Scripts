"""
This was put here in a known broken state. It was mainly uploaded at the request of a few people so they had something to work off. In due time I'll come back to this...

"""
import requests
import csv
import os
import time
import json
import feedparser
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse

##### - Allow the user to input their full ODPS URL so that we don't have to ask for IP + API key. 
url = input("Paste in your full ODPS URL from your Kavita user dashboard (/preferences#clients): ")

parsed_url = urlparse(url)

host_address = parsed_url.scheme + "://" + parsed_url.netloc
api_key = parsed_url.path.split('/')[-1]

print("Host Address:", host_address)
print("API Key:", api_key)
print("---------------------------")

login_endpoint = "/api/Plugin/authenticate" # Don't change
search_endpoint = "/api/Library/list?path=/manga" #Change to match your path you want to monitor
try:
    apikeylogin = requests.post(host_address + login_endpoint + "?apiKey=" + api_key + "&pluginName=CheckSeries")
    apikeylogin.raise_for_status()
    jwt_token = apikeylogin.json()['token']
#    print("JWT Token:", jwt_token) # Only for debug 
except requests.exceptions.RequestException as e:
    print("Error during authentication:", e)
    exit()

headers = {
    "Authorization": f"Bearer {jwt_token}",
    "Content-Type": "application/json"
}

response = requests.get(host_address + search_endpoint, headers=headers)

if response.status_code != 200:
    raise Exception(f"API call failed: {response.status_code}")

series_list = response.json()

""" Debug for checking the response 
# Save the JSON response directly to a file
with open("series_list.json", "w") as jsonfile:
    json.dump(series_list, jsonfile, indent=4)
"""

sorted_series_list = sorted(series_list, key=lambda x: x["name"])

# Create a CSV file
with open("series_list.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
    writer.writerow(["Title", "Full Path"])
    for series in sorted_series_list:
        title = series["name"]
        full_path = series["fullPath"]
        
        writer.writerow([title, full_path])
        
# Read the series titles from the CSV and store them in a set
series_titles = set()
with open("series_list.csv", "r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        series_titles.add(row["Title"])

# RSS Caching to not hammer nyaa
CACHE_FILE = "rss_cache.xml"
CACHE_EXPIRATION = 900  # 15 minutes in seconds

# Check if the cache file exists and if it's still valid
if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE)) < CACHE_EXPIRATION:
    with open(CACHE_FILE, "r") as file:
        rss_feed = file.read()
else:
    # Fetch the RSS feed from the URL
    response = requests.get("https://nyaa.si/?page=rss&c=3_1")
    rss_feed = response.text
    
    # Cache the result
    with open(CACHE_FILE, "w") as file:
        file.write(rss_feed)

# Parse the RSS feed
feed = feedparser.parse(rss_feed)

# Process the feed entries
for entry in feed.entries:
#    print("Entry:", entry)  # Print the entire entry object for debugging
    rss_title = entry.title

    # Check if the RSS title matches any series title from the CSV
    # TODO: matching needs to be cleaned up. It should be more percise so it doesn't just match a series based on the first word. Example: 'Kingdom Hearts' will match against 'Kingdom' 
    matching_series = [series_title for series_title in series_titles if series_title in rss_title]

    if matching_series:
        print(f"Matching series found: {matching_series[0]}")
#        print(f"Link: {entry.link}")
        
        # Use BeautifulSoup to remove HTML tags from entry.description
        description_html = entry.description
        description_text = BeautifulSoup(description_html, "html.parser").get_text()
        
        print(f"Description: {description_text}")
        
         # Access nyaa:infoHash
        nyaa_info_hash = entry.get("nyaa_infohash")
        if nyaa_info_hash:
            print(f"nyaa:infoHash: {nyaa_info_hash}")
        
        # Access nyaa:size
        nyaa_size = entry.get("nyaa_size")
        if nyaa_size:
            print(f"nyaa:size: {nyaa_size}")
        
        # Extract series name, volume number, and chapter number using regular expressions
        # TODO: Volume / Chapter matching needs to improve. 
        series_info_match = re.search(r'(.*?)\s*(?:(?:V(\d+))|(?:Volume (\d+))|(V(\d+)-(\d+))|(Chapter (\d+)))', description_text)

        if series_info_match:
            series_name = {matching_series[0]}
            volume_number = int(series_info_match.group(2)) if series_info_match.group(2) else None
            chapter_number = int(series_info_match.group(3)) if series_info_match.group(3) else None

            print(f"Series Name: {series_name}")
            print(f"Volume Number: {volume_number}")
            print(f"Chapter Number: {chapter_number}")
        else:
            print("Match not found")

 
        #  Extract series name and number using regular expressions
        series_name_match = re.search(r'(.*?)(\d+\.\d+)', description_text)
        if series_name_match:
            series_name = series_name_match.group(1).strip()
            series_number = float(series_name_match.group(2))
            print(f"Series Name: {series_name}")
            print(f"Series Number: {series_number}")
        
        print("-----")
