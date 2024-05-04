"""
Author: DieselTech
URL: https://github.com/DieselTech/Comic-Management-Scripts
Date created: May 3, 2024, 15:30 PM

Description: This will create folders and cbz files to simulate a comic book library. Creates a folder
for each entry in `publishers` and a sub-folder using a random entry in `prefixes`. Each sub-folder will contain a cbz file
with a random name from `base_filenames` that has the single image that is defined in `image_file`.

Software requirements:
- Python 3
- zipfile

Usage:
python create_fake_comic_library.py
"""

import os
import random
import zipfile

# Top level directory where folders will be created, relative to the path this script was ran.
top_level_directory = "Comic_Book_Publishers"

# Image file to be used in zips. Have it in the same folder as this script. 
image_file = "sample_image.jpg"

publishers = [
    "Marvel",
    "DC Comics",
    "Image",
    "Dark Horse Comics",
    "IDW Publishing",
    "BOOM! Studios",
    "Valiant",
    "Dynamite Entertainment",
    "Archie Comics",
    "Oni Press"
]

prefixes = [
    "Mighty",
    "In other world with my",
    "Farming in another world",
    "I became the strongest",
    "The name of this series is so long I'm worried that windows won't be able to handle it",
    "Truck-kun is coming for you",
    "Epic",
    "Super"
]

base_filenames = [
    "Chapter",
    "Volume",
    "Ch",
    "Vol",
    "Special",
    "Light Novel",
    "Series"
]

def generate_random_filename():
    base_name = random.choice(base_filenames)
    random_number = random.randint(1, 20)
    return f"{base_name} {random_number:03}.cbz"


if not os.path.exists(top_level_directory):
    os.makedirs(top_level_directory)


def create_random_directory(publisher):
    random_prefix = random.choice(prefixes)
    random_name = random_prefix + " " + ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=6))
    publisher_directory = os.path.join(top_level_directory, publisher)
    random_directory = os.path.join(publisher_directory, random_name)

    if not os.path.exists(publisher_directory):
        os.makedirs(publisher_directory)

    os.makedirs(random_directory)
    zip_filename = generate_random_filename()

    with zipfile.ZipFile(os.path.join(random_directory, zip_filename), 'w') as zip_file:
        zip_file.write(image_file, os.path.basename(image_file))

for publisher in publishers:
    create_random_directory(publisher)

print("Folders and files created successfully.")
