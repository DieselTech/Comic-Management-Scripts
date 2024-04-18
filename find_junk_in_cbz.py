"""
Author: DieselTech
URL: https://github.com/DieselTech/Comic-Management-Scripts
Date created: April 18, 2024

Description:
Scans a path the user supplies for cbz files and records how many "junk" files are inside. Junk files are considered anything not an image or .xml metadata. Stores results in non_image_files.log file that is created alongside the script.


Software requirements:
- Python 3

Usage:
python find_junk_in_cbz.py.py
"""

import os
import zipfile
import glob

library_path = input("Enter the path to your library files: ")

cbz_files = glob.glob(os.path.join(library_path, '**/*.cbz'), recursive=True)

total_non_image_files = 0
total_non_image_size = 0
extension_count = {}
extension_size = {}

log_file_path = "non_image_files.log"
with open(log_file_path, "w") as log_file:
    for cbz_file_path in cbz_files:
        with zipfile.ZipFile(cbz_file_path, 'r') as cbz_file:
            for info in cbz_file.infolist():
                # Exclude directory entries and check for non-image extensions
                if not info.is_dir() and not info.filename.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".xml", ".webp", ".avif")):
                    total_non_image_size += info.file_size
                    total_non_image_files += 1
                    log_file.write(f"{os.path.join(cbz_file_path, info.filename)}\n")
                    _, ext = os.path.splitext(info.filename)
                    ext = ext.lower()
                    # Apple is special
                    if not ext:
                        ext = '.DS_Store'
                    extension_count[ext] = extension_count.get(ext, 0) + 1
                    extension_size[ext] = extension_size.get(ext, 0) + info.file_size

    # Sort the extension_count dictionary by count values in descending order
    sorted_extension_count = dict(sorted(extension_count.items(), key=lambda x: x[1], reverse=True))

    log_file.write("\nExtension Type Statistics (Ordered by Count):\n")
    for ext, count in sorted_extension_count.items():
        size_bytes = extension_size.get(ext, 0)
        size_mb = size_bytes / (1024 * 1024)
        log_file.write(f"{ext}: {count} times, Total Size: {size_bytes} bytes ({size_mb:.2f} MB)\n")

print(f"Total non-image files found: {total_non_image_files}")
print(f"Total size of non-image files: {total_non_image_size} bytes")
print(f"Non-image files list recorded in {log_file_path}")

print("\nExtension Type Statistics:")
for ext, count in extension_count.items():
    print(f"{ext}: {count} times")
