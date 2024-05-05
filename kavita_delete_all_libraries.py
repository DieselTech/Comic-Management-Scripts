"""
Author: DieselTech
URL: https://github.com/DieselTech/Comic-Management-Scripts
Date created: May 4, 2024, 23:30 PM

Description: This unbiasly deletes every library on the server. There is no customization of what it deletes. Just kills it all. 

Software requirements:
- Python 3
- requests

Usage:
python kavita_delete_all_libraries.py

     _.-^^---....,,--
 _--                  --_
<                        >)
|                         |
 \._                   _./
    ```--. . , ; .--'''
          | |   |
       .-=||  | |=-.
       `-=#$%&%$#=-'
          | ;  :|
 _____.,-#%&$@%#&#~,._____


"""

import random
import time
from urllib.parse import urlparse

import requests


def authenticate(url):
    parsed_url = urlparse(url)
    host_address = parsed_url.scheme + "://" + parsed_url.netloc

    api_key = parsed_url.path.split('/')[-1]
    login_endpoint = "/api/Plugin/authenticate"
    try:
        apikeylogin = requests.post(
            host_address + login_endpoint + "?apiKey=" + api_key + "&pluginName=pyNuke")
        apikeylogin.raise_for_status()
        jwt_token = apikeylogin.json()['token']
        return jwt_token, host_address
    except requests.exceptions.RequestException as e:
        print("Error during authentication:", e)
        exit()


def get_all_libraries(jwt_token, host_address):
    get_all_libraries_endpoint = "/api/Library/libraries"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(host_address + get_all_libraries_endpoint, headers=headers)
    if response.status_code != 200:
        print("Error: Failed to get data from API.")
        return
    # take the response and store the library IDs in a list
    library_ids = []
    for library in response.json():
        library_ids.append(library['id'])
    return library_ids


def delete_all_libraries(jwt_token, host_address):
    delete_endpoint = "/api/Library/delete"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    # take the library IDs and delete them
    library_ids = get_all_libraries(jwt_token, host_address)

    for library_id in library_ids:
        string_it = str(library_id)
        build_url = host_address + delete_endpoint + "?libraryid=" + string_it
        response = requests.delete(build_url, headers=headers)
        if response.status_code != 200:
            print("Error: Failed to delete data from API.")
        else:
            print(f"Library '{library_id}' deleted. Response: {response.status_code} - Sleeping before next launch")
            time.sleep(0.55)
    return


def main():
    sure = input("Are you sure you want to delete all libraries? (yes/no): ")
    if sure.lower() == "yes":
        really_sure = input("Are you really sure? (yup/nope): ")
        if really_sure.lower() == "yup":
            print("Take a few seconds to think about it before dropping to DEFCON 1")
            time.sleep(random.randint(3, 8))
            url = input("Enter the full OPDS URL you want to nuke the libraries from: ")
            jwt_token, host_address = authenticate(url)
            get_all_libraries(jwt_token, host_address)
            delete_all_libraries(jwt_token, host_address)
        else:
            print("Back to DEFCON 5")
            exit()


if __name__ == "__main__":
    main()
