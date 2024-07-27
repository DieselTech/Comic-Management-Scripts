"""
Author: DieselTech
URL: https://github.com/DieselTech/Kavita-API-Scripts
Date created: May 5, 2024, 14:30 PM

Description:
This will look at a root folder and add all sub-folders to your Kavita instance as their own library.

It's a bit more convoluted when dealing with docker containers but I'll include an example here:
- You have your files on a network share on your Windows machine at M:\comics
- You have a docker container running Kavita and you have your files mapped to the container as /comics
- That means dockers internal path of /comics and your windows share of M:\comics is exactly the same.
- This would let you run the script in windows, connect to your Kavita instance and add all the folders as a library.
- When on windows and it asks for the local folder name, it's just that. The name like "comic_folder". No drive letter.

Software requirements:
- Python 3 or later
- requests
- argparse
- urllib
- time

Usage:
python create_libraries_from_folders.py
"""

import argparse
import requests
from urllib.parse import urlparse
import os
import time

global_ignore_folders = [".zzz_check", "@eaDir", "@Recycle", "#recycle"]


def authenticate(url):
    parsed_url = urlparse(url)
    host_address = parsed_url.scheme + "://" + parsed_url.netloc

    api_key = parsed_url.path.split('/')[-1]
    login_endpoint = "/api/Plugin/authenticate"
    try:
        apikeylogin = requests.post(
            host_address + login_endpoint + "?apiKey=" + api_key + "&pluginName=pyFolderAddScript")
        apikeylogin.raise_for_status()
        jwt_token = apikeylogin.json()['token']
        return jwt_token, host_address
    except requests.exceptions.RequestException as e:
        print("Error during authentication:", e)
        exit()


def get_docker_path(local_path, docker_path):
    publisher_folder = os.path.split(local_path)[-1]
    docker_path = os.path.join(docker_path, publisher_folder)
    return docker_path


def submit_folders(jwt_token, host_address, path, exclude_list, library_type, docker_modifier):
    addlib_endpoint = "/api/Library/create"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

    for entry in os.scandir(path):
        if entry.is_dir():
            if entry.name.lower() in global_ignore_folders:
                print(f"Skipping folder '{entry.name}' due to global exclusion.")
                continue
            if exclude_list is not None and entry.name.lower() in [name.lower() for name in exclude_list]:
                print(f"Skipping folder '{entry.name}' due to exclusion.")
                continue
        if docker_modifier is None and entry.name not in exclude_list:
            payload = {
                "name":entry.name,
                "type":library_type,
                "folders": [f'{entry.path}'],
                "folderWatching": True,
                "includeInDashboard": True,
                "includeInRecommended": True,
                "includeInSearch": True,
                "manageCollections": True,
                "manageReadingLists": True,
                "allowScrobbling": True,
                "fileGroupTypes": [1],
                "excludePatterns": [""]
            }
            print(f'No Docker Modifier Found')
            response = requests.post(host_address + addlib_endpoint, headers=headers, json=payload)
            if response.status_code != 200:
                print("Error: Failed to post data to API.")
                return
            print(f"Folder '{entry.name}' sent. Response: {response.status_code}")
            time.sleep(0.5)
        else:
            if entry.is_dir():
                if entry.name.lower() in [name.lower() for name in exclude_list]:
                    print(f"Skipping folder '{entry.name}' due to exclusion.")
                    continue
                docker_path = get_docker_path(entry.path, docker_modifier)
                payload = {
                    "name": entry.name,
                    "type": library_type,
                    "folders": [docker_path],
                    "folderWatching": True,
                    "includeInDashboard": True,
                    "includeInRecommended": True,
                    "includeInSearch": True,
                    "manageCollections": True,
                    "manageReadingLists": True,
                    "allowScrobbling": True,
                    "fileGroupTypes": [1],
                    "excludePatterns": [""]
                }
                print(f'🐳 Docker Modifier Found 🐳')
                response = requests.post(host_address + addlib_endpoint, headers=headers, json=payload)
                if response.status_code != 200:
                    print("Error: Failed to post data to API.")
                    return
                print(f"Folder '{entry.name}' sent. Response: {response.status_code}")
                print()
                time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description='Automate the script from the command line.')

    parser.add_argument('-u', '--url', type=str, required=False, help='Full ODPS URL from your Kavita user dashboard ('
                                                                      '/preferences#clients)')

    parser.add_argument('-p', '--path', type=str, required=False, help='Path to your folders')

    parser.add_argument('-lt', '--library-type', type=int, required=False, choices=range(0, 6),
                        help='What type of library is this? 0 = Manga, 1 = Comics, 2 = Books (epubs), 3 = Loose '
                             'Images, 4 = Light Novels, 5 = Comics(ComicVine)')

    parser.add_argument('-d', '--docker-modifier', type=str, required=False, help='Modifier to correct the path to '
                                                                                  'the comics folder within the '
                                                                                  'docker container.')

    parser.add_argument('-e', '--exclude', nargs='+', default=[], help='Publisher names to exclude')

    args = parser.parse_args()
    docker = 0

    if not any(vars(args).values()):
        docker_question = input("Is your Kavita instance running in a container? (y/n): ")
        if docker_question.lower() == 'y':
            docker = 1
            docker_question2 = input("Are you running this script from inside said container? (y/n): ")
            if docker_question2.lower() == 'y':
                print("Running from inside the container. No need to modify the path.")
                docker = 0
    else:
        docker = 0

    if args.path is None:
        if docker == 0:
            args.path = input("Enter the path to the folder containing your files: ")
        elif docker == 1:
            path_question = input("Is the machine your running this on Windows or Linux? (w/l): ").lower()
            if path_question == 'w':
                args.path = input("Enter the drive letter containing your files (e.g. C:\\): ") + \
                            input("Enter the path to the local folder containing your files: ")
            elif path_question == 'l':
                args.path = input("Enter the path to the local folder containing your files: ")
            else:
                print("Invalid input. Please enter 'w' or 'l'.")
                exit()
    else:
        args.path = input('Enter the path to the folder containing your files: ')

    if args.library_type is None:
        args.library_type = int(input("What type of library is this? 0 = Manga, 1 = Comics, 2 = Books (epubs), "
                                      "3 = LooseImages, 4 = Light Novels, 5 = Comics(ComicVine): "))

    if args.docker_modifier is None:
        if docker == 1:
            args.docker_modifier = input("What is your path inside the the container? (e.g. /comics)")
        else:
            args.docker_modifier = None

    if not args.exclude:
        exclude = input("Do you need to exclude any publisher folders? (y/n): ")
        if exclude.lower() == 'y':
            print("Enter the publisher names to exclude, one per line. Send a blank line when finished: ")
            args.exclude = []
            while True:
                line = input()
                if line:
                    args.exclude.append(line)
                else:
                    break
        else:
            args.exclude = []

    if args.url is None:
        args.url = input("Paste in your full ODPS URL from your Kavita user dashboard (/preferences#clients): ")

    jwt_token, host_address = authenticate(args.url)
    submit_folders(jwt_token, host_address, args.path, args.exclude, args.library_type, args.docker_modifier)


if __name__ == "__main__":
    main()
