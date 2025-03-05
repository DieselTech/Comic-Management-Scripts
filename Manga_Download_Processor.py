"""
Manga Download Processor
This script processes downloaded manga files, converting images to WebP format and moving them to a library directory.
Creation Date: 2025-03-05 18:20

Author: DieselTech

"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler
import glob
import subprocess
import re
import shutil
import zipfile
from PIL import Image
from datetime import date
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

patterns = [
    ('Ch', re.compile(r'(\b|_)(c|ch)(\.?\s?)(?P<Chapter>(\d+(\.\d)?)(-c?\d+(\.\d)?)?)')),
    ('Ch_bare', re.compile(r'^(?P<Series>.+?)(?<!Vol)(?<!Vol.)(?<!Volume)\s(\d\s)?(?P<Chapter>\d+(?:\.\d+|-\d+)?)(?:\s\(\d{4}\))?(\b|_|-)')),
    ('Ch_bare2', re.compile(r'^(?!Vol)(?P<Series>.*)\s?(?<!vol\. )\sChapter\s(?P<Chapter>\d+(?:\.?[\d-]+)?)')),
    ('Volume', re.compile(r'(?P<Title>.+?)\s(?:v|V)(?P<Volume>\d+)(?:\s-\s(?P<Extra>.*?))?\s*(?:\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('ChapterExtras', re.compile(r'(?P<Title>.+?)(?=\s+(?:c|ch|chapter)\b|\s+c\d)(?:.*?(?:c|ch|chapter))?\s*(?P<Chapter>\d+(?:\.\d+)?)?(?:\s-\s(?P<Extra>.*?))?(?:\s*\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Chapter', re.compile(r'(?P<Title>.+?)\s(?:(?:c|ch|chapter)?\s*(?P<Chapter>\d+(?:\.\d+)?))?(?:\s-\s(?P<Extra>.*?))?\s*(?:\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Simple_Ch', re.compile(r'Chapter(?P<Chapter>\d+(-\d+)?)')),
    ('Vol_Chp', re.compile(r'(?P<Series>.*)(\s|_)(vol\d+)?(\s|_)Chp\.? ?(?P<Chapter>\d+)')),
    ('V_Ch', re.compile(r'v\d+\.(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)')),
    ('Bare_Ch', re.compile(r'^((?!v|vo|vol|Volume).)*(\s|_)(?P<Chapter>\.?\d+(?:.\d+|-\d+)?)(?P<Part>b)?(\s|_|\[|\()')),
    ('Vol_Chapter', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(Chp|Chapter)\.?(\s|_)?(?P<Chapter>\d+)')),
    ('Vol_Chapter2', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+)')),
    ('Vol_Chapter3', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)')),
    ('Vol_Chapter4', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)(\s|_)(?P<Extra>.*?)')),
    ('Complex_Series', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3})\s+\(\d{4}\)')),
    ('Complex_Series2', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3})\s+\(\d{4}\)\s(?P<Extra>.+?)')),
    ('Complex_SeriesDecimal', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3}(?:\.\d+)?)\s+\(\d{4}\)')),
 #   ('Monolith', re.compile(r'(?P<Title>.+?)\s(?:(?:c|ch|chapter)?\s*(?P<Chapter>\d+(?:\.\d+)?))(?:\s-\s(?P<Extra>.*?))?(?:\s*\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))')),
    ('Vol_Chapter5', re.compile(r'(\b|_)(c|ch)(\.?\s?)(?P<Chapter>(\d+(\.\d)?)(-c?\d+(\.\d)?)?)'))
]

def setup_logging():
    # Create logger
    logger = logging.getLogger('MangaUnpacker')
    logger.setLevel(logging.DEBUG)

    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(message)s')

    # File handler (daily rotation)
    log_file = os.path.join(os.path.dirname(__file__), 'logs', 'MangaUnpacker.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def move_to_finished(download_directory):
    """Moves successfully processed folders from download_directory to '!Finished'."""
    finished_dir = os.path.join(download_directory, "!Finished")
    os.makedirs(finished_dir, exist_ok=True)

    for folder in os.listdir(download_directory):
        source_folder = os.path.join(download_directory, folder)
        destination_folder = os.path.join(finished_dir, folder)

        # Skip special folders
        if folder in ["!Finished", "!temp_processing", "!temp_extract"]:
            continue
        
        # Move only directories
        if os.path.isdir(source_folder):
            try:
                shutil.move(source_folder, destination_folder)
                print(f"✔ Successfully moved '{source_folder}' to '!Finished'.")
            except Exception as e:
                print(f"⚠ Error moving '{source_folder}' to '!Finished': {e}")

def count_non_none_non_blank_keys(match_dict):
#    print(sum(1 for key, value in match_dict.items() if value))
    return sum(1 for key, value in match_dict.items() if value)

def match_best_pattern(filename):
    best_matches = []
    best_count = 0
    
    for pattern_name, pattern in patterns:
        match = pattern.match(filename)
        if match:
            match_dict = match.groupdict()
            count = count_non_none_non_blank_keys(match_dict)
            
            if count > best_count:
                best_count = count
                best_matches = [(pattern_name, match_dict)]
            elif count == best_count and count > 0:
                best_matches.append((pattern_name, match_dict))
    
    if not best_matches:
        return 'None', {}
        
    if len(best_matches) > 1:
        print(f"\nMultiple matches found for: {filename}")
        for i, (pattern_name, match_dict) in enumerate(best_matches):
            print(f"{i+1}. Pattern: {pattern_name}")
            print(f"   Match: {match_dict}")
        
        choice = input(f"\nSelect pattern number (1-{len(best_matches)}) or enter 'M' to manually specify: ").strip()

        if choice.lower() == 'm':  
            manual_series = input("Enter series name: ").strip()
            manual_chapter = input("Enter chapter number (or press Enter to skip): ").strip()
            match_dict = {'Series': manual_series, 'Chapter': manual_chapter} if manual_chapter else {'Series': manual_series}
            return ("Manual Entry", match_dict)

        return best_matches[int(choice) - 1]

    elif len(best_matches) == 0:
        print(f"\nNo matches found for: {filename}")
        manual_choice = input("Enter 'M' to manually specify details, 'S' to skip this file: ").strip().lower()

        if manual_choice == 'm':
            manual_series = input("Enter series name: ").strip()
            manual_chapter = input("Enter chapter number (or press Enter to skip): ").strip()
            match_dict = {'Series': manual_series, 'Chapter': manual_chapter} if manual_chapter else {'Series': manual_series}
            return ("Manual Entry", match_dict)
        else:
            print(f"Skipping file: {filename}")
            return None  # Indicating that the file should be skipped

    
    return best_matches[0]

def extract_rars(folder_path):
    """Extract RAR files from a single folder."""
    extracted_folders = []
    num_extracted = 0
    
    # Only look for RAR files in this specific folder, not recursively
    for file in os.listdir(folder_path):
        if file.endswith('.rar'):
            rar_path = os.path.join(folder_path, file)
            folder_name = os.path.join(folder_path, "!temp_extract")
            logger.info(f"Creating extraction folder: {folder_name}")
            
            if os.path.exists(folder_name):
                shutil.rmtree(folder_name)
            os.makedirs(folder_name)
            
            subprocess.call(['unrar', 'x', rar_path, folder_name])
            num_extracted = len([f for f in os.listdir(folder_name) 
                               if os.path.isfile(os.path.join(folder_name, f))])
            logger.info(f"Extracted {num_extracted} files to: {folder_name}")
            extracted_folders.append(folder_name)
            
    return extracted_folders, num_extracted

def cleanup_files(temp_dir, filepath, new_filepath, library_path, extracted_folders):
    """Clean up all temporary files and folders created during conversion."""
    try:
        # Log all folders that will be cleaned
        logger.info("Cleanup starting...")
        logger.info(f"Temp dir: {temp_dir}")
        logger.info(f"Extracted folders to clean: {extracted_folders}")
        
        if not extracted_folders:
            logger.info("No extracted folders found for cleanup.")
        
        # Clean up temp directory
        if os.path.exists(temp_dir):
            logger.info(f"Removing temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
        
        # Clean extracted RAR folders
        for folder in extracted_folders:
            abs_folder = os.path.abspath(folder)
            if os.path.exists(abs_folder):
                logger.info(f"Removing RAR folder: {abs_folder}")
                shutil.rmtree(abs_folder)
            else:
                logger.warning(f"Folder not found: {abs_folder}")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def process_directory(download_directory, library_path):
    """Process manga files and move completed series folders to !Finished."""
    success = True
    processed_folders = set()
    
    # Create temp directory for processing
    temp_dir = os.path.join(download_directory, "!temp_processing")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    # Walk through directory tree
    for root, _, files in os.walk(download_directory):
        # Skip special folders
        if any(special in root for special in ["!Finished", "!temp_processing", "!temp_extract"]):
            continue
            
        # First handle RAR files if present
        rar_files = [f for f in files if f.endswith('.rar')]
        if rar_files:
            extracted_folders, num_extracted = extract_rars(root)
            if num_extracted > 0:
                # Process extracted files
                for extract_dir in extracted_folders:
                    for extracted_file in os.listdir(extract_dir):
                        if extracted_file.endswith('.cbz'):
                            filepath = os.path.join(extract_dir, extracted_file)
                            filename = os.path.basename(filepath)
                            try:
                                # Process the extracted CBZ
                                match_type, match_dict = match_best_pattern(filename)
                                logger.info(f"\nProcessing extracted: {filename}")
                                logger.info(f"Match Type: {match_type}")
                                logger.info(f"Match: {match_dict}")

                                if not match_dict or (not match_dict.get('Title') and not match_dict.get('Series')):
                                    logger.warning(f"Could not determine series for {filename}")
                                    continue

                                # Process extracted CBZ file
                                success &= process_cbz_file(filepath, temp_dir, library_path, match_dict)
                            except Exception as e:
                                logger.error(f"Error processing extracted file {filepath}: {str(e)}")
                                success = False

                # Clean up extraction folders
                for folder in extracted_folders:
                    if os.path.exists(folder):
                        shutil.rmtree(folder)

        # Then handle regular CBZ files
        cbz_files = [f for f in files if f.endswith('.cbz')]
        for file in cbz_files:
            filepath = os.path.join(root, file)
            filename = os.path.basename(filepath)
            try:
                match_type, match_dict = match_best_pattern(filename)
                logger.info(f"\nProcessing: {filename}")
                logger.info(f"Match Type: {match_type}")
                logger.info(f"Match: {match_dict}")

                if not match_dict or (not match_dict.get('Title') and not match_dict.get('Series')):
                    logger.warning(f"Could not determine series for {filename}")
                    continue

                # Process the CBZ file
                success &= process_cbz_file(filepath, temp_dir, library_path, match_dict)
            except Exception as e:
                logger.error(f"Error processing {filepath}: {str(e)}")
                success = False

        # Move folder to !Finished if all files processed
        if root != download_directory:
            finished_dir = os.path.join(download_directory, "!Finished")
            os.makedirs(finished_dir, exist_ok=True)
            folder_name = os.path.basename(root)
            dest_path = os.path.join(finished_dir, folder_name)
            try:
                shutil.move(root, dest_path)
                logger.info(f"Moved completed folder '{folder_name}' to !Finished")
            except Exception as e:
                logger.error(f"Error moving folder '{folder_name}' to !Finished: {str(e)}")

    # Final cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    return success

def process_cbz_file(filepath, temp_dir, library_path, match_dict):
    """Process a single CBZ file."""
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        new_filepath = os.path.join(temp_dir, os.path.basename(filepath).replace('.cbz', '_webp.cbz'))
        with zipfile.ZipFile(new_filepath, 'w') as new_zip:
            # Convert images to WebP
            def convert_image(img_path):
                with Image.open(img_path) as img:
                    webp_path = os.path.splitext(img_path)[0] + '.webp'
                    img.save(webp_path, 'WEBP', quality=80)
                    return webp_path

            image_files = []
            for _, _, temp_files in os.walk(temp_dir):
                for f in temp_files:
                    if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                        image_files.append(os.path.join(temp_dir, f))

            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(convert_image, img_path) for img_path in image_files]
                for future in tqdm(as_completed(futures), total=len(image_files), desc="Converting to WebP"):
                    webp_path = future.result()
                    new_zip.write(webp_path, os.path.basename(webp_path))

        # Move processed file to library
        move_to_library(new_filepath, library_path,
                      match_dict.get('Title') or match_dict.get('Series'),
                      match_dict.get('Volume'),
                      match_dict.get('Chapter'))

        # Clean up temp files
        for item in os.listdir(temp_dir):
            item_path = os.path.join(temp_dir, item)
            if os.path.isfile(item_path):
                os.remove(item_path)

        return True
    except Exception as e:
        logger.error(f"Error in process_cbz_file: {str(e)}")
        return False

def series_exists(library_path, series_name):
    series_path = os.path.join(library_path, series_name)
    return os.path.exists(series_path)

def move_to_library(source_file, library_path, series_name, volume=None, chapter=None):
    series_path = os.path.join(library_path, series_name)

    # Create series directory if it doesn't exist
    if not os.path.exists(series_path):
        os.makedirs(series_path)

    # Construct the new file name
    file_name = series_name
    if volume:
        file_name += f" v{volume}"
    if chapter:
        file_name += f" - Chapter {chapter}"
    file_name += os.path.splitext(source_file)[1]  # Add the file extension

    dest_path = os.path.join(series_path, file_name)

    # Check if source is directly in process directory
    source_dir = os.path.dirname(source_file)
    is_root_file = os.path.samefile(source_dir, os.path.dirname(os.path.abspath(source_file)))

    # Handle file already exists case
    if os.path.exists(dest_path):
        # Check if source file has (F) in it
        if "(F)" in os.path.basename(source_file):
            # Replace existing file with the (F) version
            os.remove(dest_path)
            shutil.move(source_file, dest_path)
            logger.warning(f"Overwriting {dest_path} with {source_file} as it is a (F) version")
        else:
            conflicts_path = os.path.join(download_directory, "!Conflicts")
            if not os.path.exists(conflicts_path):
                os.makedirs(conflicts_path)
            conflict_dest_path = os.path.join(conflicts_path, file_name)
            counter = 1
            while os.path.exists(conflict_dest_path):
                base, ext = os.path.splitext(file_name)
                conflict_dest_path = os.path.join(conflicts_path, f"{base}_{counter}{ext}")
                logger.warning(f"Conflict: {file_name} already exists. Moving to {conflict_dest_path}")
                counter += 1
            dest_path = conflict_dest_path
    
    # Copy or move the file based on location
    if is_root_file:
        shutil.copy2(source_file, dest_path)
        logger.info(f"Copied {source_file} to {dest_path}")
    else:
        shutil.move(source_file, dest_path)
        logger.info(f"Moved {source_file} to {dest_path}")

def get_library_path():
    while True:
        library_path = input("Enter your comic library path: ")
        if os.path.exists(library_path):
            return library_path
        print(f"Error: Path '{library_path}' does not exist")

def get_download_directory():
    while True:
        download_directory = input("Enter the directory path with raw downloads to process: ")
        if os.path.exists(download_directory):
            return download_directory
        print(f"Error: Directory '{download_directory}' does not exist")

if __name__ == '__main__':
    logger = setup_logging()
    library_path = get_library_path()
    download_directory = get_download_directory()
    process_directory(download_directory, library_path)