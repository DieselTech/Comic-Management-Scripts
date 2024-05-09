"""
Author: DieselTech
URL: https://github.com/DieselTech/Comic-Management-Scripts
Date created: May 8, 2024, 21:30 PM

Description: This will mimic an exist folder and file layout you already have on disk into a new folder. Doesn't copy any actual data. Just creates 0 byte files. 
This is mainly useful when your testing scripts that do operations on files based on certain conditions and you don't want to wait for file copies over and over as you test and iterate. 

Software requirements:
- Python 3
- zipfile

Usage:
python mimic_files_and_folder_strcuture.py <directory> <new_directory>
"""

import os
import sys


def mimic_folder_structure(directory, new_directory):
    print("Fake it until you make it")
    for root, dirs, files in os.walk(directory):
        for file in files:
            old_name = os.path.join(root, file)
            new_path = os.path.join(new_directory, os.path.relpath(old_name, directory))
            #print(f"Old name: {old_name}")
            #print(f"New name: {new_path}")
            try:
                os.makedirs(os.path.dirname(new_path), exist_ok=True)
                with open(new_path, 'w') as f:
                    pass
            except Exception as e:
                print(f"Error creating '{new_path}': {e}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python mimic_folder_structure.py <directory> <new_directory>")
        sys.exit(1)

    directory = sys.argv[1]
    new_directory = sys.argv[2]

    mimic_folder_structure(directory, new_directory)
    print("We made it")

