"""
Manga Unpacker and Library Manager
=================================

A tool for organizing and processing manga/comic files by automatically detecting series names, 
chapter numbers, and volume information from filenames. This script can extract compressed files, 
convert images to WebP format for space savings, and organize files into a structured library.

Did I break it?:
--------------

If something isn't working correctly or you are getting bad matches, let me know by opening an issue.
Helping improve the regex patterns will help everyone in down the line. Also, I know a lot of this code
is crap. Especially the part around processing directories. I'll get around to making it better at some point.


Requirements:
------------
- Python 3.6+
- Dependencies: PIL (Pillow), tqdm, unrar (external command-line tool)
  Install with: pip install Pillow tqdm

- The unrar command-line tool must be installed separately:
  • Windows: Download from https://www.rarlab.com/rar_add.htm
  • Linux: sudo apt install unrar (Ubuntu/Debian) or sudo dnf install unrar (Fedora)
  • macOS: brew install unrar (using Homebrew)
  
Usage:
-----
python Manga_Download_Processor.py [options]

Arguments:
---------
  --dry-run, -d       Test pattern matching without moving files
  --test, -t          Run pattern tests on sample filenames
  --auto, -a          Run in automatic mode without user prompts (for scheduled jobs)
  --source, -s DIR    Source directory with raw downloads to process
  --dest, -l DIR      Destination library path for processed files
  
Examples:
--------
# Interactive mode (prompted for paths):
python Manga_Download_Processor.py

# Dry run to preview what would happen:
python Manga_Download_Processor.py --dry-run --source /path/to/downloads --dest /path/to/library

# Test pattern matching on specific filenames:
python Manga_Download_Processor.py --test "My Series c10 (2023).cbz" "Another Example v02 (2022).cbz"

# Fully automated mode for scheduled jobs:
python Manga_Download_Processor.py --auto --source /path/to/downloads --dest /path/to/library

# Combine automatic mode with dry run for testing:
python Manga_Download_Processor.py --auto --dry-run --source /path/to/downloads --dest /path/to/library

Pattern Matching System:
----------------------
The script uses regex patterns to detect series names, chapter numbers, and volume information
from filenames. A weighted scoring system ranks potential matches with these criteria:

- Series name and title are most important (10 points)
- Chapter numbers are next (8 points)
- Volume numbers are valued (6 points)
- Additional info like year and extras are also considered

Quality checks are applied to penalize unrealistic matches like:
- Series names ending with "Chapter" or separators
- Unrealistic chapter numbers
- Missing essential components

Library Organization:
------------------
Files will be organized into their own series folder based on the series name that is parsed out.
If a volume number or chapter number is found, this info will be added to the filename. 

For example the filename "My Series c10 (2023).cbz" would get sorted to:

/library_path/
  /My Series/
    My Series - Chapter 10.cbz

Automation:
---------
When ran in automatic mode the first match will be chosen because human input isn't possible.
You should test the script out first before using automatic mode using the --dry-run switch to see how it would handle things. 

To run this script automatically on a schedule:

# Windows Task Scheduler example:
python c:/path/to/Manga_Download_Processor.py --auto --source "D:\Downloads\Manga" --dest "E:\Library"

# Linux cron job example:
0 3 * * * /usr/bin/python3 /path/to/Manga_Download_Processor.py --auto --source /path/to/downloads --dest /path/to/library >> /path/to/log_file.log 2>&1
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler
import argparse
import subprocess
import re   
import shutil
import zipfile
from PIL import Image
from datetime import date
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

patterns = [
    ('Complete_Series', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d+(?:\.\d+)?)\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Complex_Series', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3})\s+\((?P<Year>\d{4})\)')),
    ('Complex_SeriesDecimal', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3}(?:\.\d+)?)\s+\((?P<Year>\d{4})\)')),
    ('Complex_Series2', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3})\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))+')),
    ('Ch', re.compile(r'(\b|_)(c|ch)(\.?\s?)(?P<Chapter>(\d+(\.\d)?)(-c?\d+(\.\d)?)?)')),
    ('Ch_bare', re.compile(r'^(?P<Series>.+?)(?<!Vol)(?<!Vol.)(?<!Volume)(?<!\sCh)(?<!\sChapter)\s(\d\s)?(?P<Chapter>\d+(?:\.\d+|-\d+)?)(?:\s\(\d{4}\))?(\b|_|-)')),
    ('Ch_bare2', re.compile(r'^(?!Vol)(?P<Series>.*)\s?(?<!vol\. )\sChapter\s(?P<Chapter>\d+(?:\.?[\d-]+)?)')),
    ('Series_Dash_Ch', re.compile(r'^(?P<Series>.+?)\s-\s(?:Ch|Chapter)\.?\s(?P<Chapter>\d+(?:\.\d+)?)\.?(?:cbz|cbr)?$')),
    ('Series_Ch_Number', re.compile(r'^(?P<Series>.+?)(?<!\s-)\s(?:Ch\.?|Chapter)\s?(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Series_Chapter', re.compile(r'^(?P<Series>.+?)(?<!\s[Vv]ol)(?<!\sVolume)\sChapter\s(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Series_Vol_Ch', re.compile(r'^(?P<Series>.+?)\sv(?P<Volume>\d+)(?:\s-\s)(?:Ch\.?|Chapter)\s?(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Volume', re.compile(r'(?P<Title>.+?)\s(?:v|V)(?P<Volume>\d+)(?:\s-\s(?P<Extra>.*?))?\s*(?:\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('ChapterExtras', re.compile(r'(?P<Title>.+?)(?=\s+(?:c|ch|chapter)\b|\s+c\d)(?:.*?(?:c|ch|chapter))?\s*(?P<Chapter>\d+(?:\.\d+)?)?(?:\s-\s(?P<Extra>.*?))?(?:\s*\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Chapter', re.compile(r'(?P<Title>.+?)\s(?:(?:c|ch|chapter)?\s*(?P<Chapter>\d+(?:\.\d+)?))?(?:\s-\s(?P<Extra>.*?))?\s*(?:\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Simple_Ch', re.compile(r'Chapter(?P<Chapter>\d+(-\d+)?)')),
    ('Vol_Chp', re.compile(r'(?P<Series>.*)(\s|_)(vol\d+)?(\s|_)Chp\.? ?(?P<Chapter>\d+)')),
    ('V_Ch', re.compile(r'v\d+\.(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)')),
    ('Titled_Vol', re.compile(r'(?P<Series>.*?)\s-\sVol\.\s(?P<Volume>\d+)')),
    ('Bare_Ch', re.compile(r'^((?!v|vo|vol|Volume).)*(\s|_)(?P<Chapter>\.?\d+(?:.\d+|-\d+)?)(?P<Part>b)?(\s|_|\[|\()')),
    ('Vol_Chapter', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(Chp|Chapter)\.?(\s|_)?(?P<Chapter>\d+)')),
    ('Vol_Chapter2', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+)')),
    ('Vol_Chapter3', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)')),
    ('Vol_Chapter4', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)(\s|_)(?P<Extra>.*?)')),
    ('Vol_Chapter5', re.compile(r'(\b|_)(c|ch)(\.?\s?)(?P<Chapter>(\d+(\.\d)?)(-c?\d+(\.\d)?)?)')),
    ('Monolith', re.compile(r'(?P<Title>.+?)\s(?:(?:c|ch|chapter)?\s*(?P<Chapter>\d+(?:\.\d+)?))(?:\s-\s(?P<Extra>.*?))?(?:\s*\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))')),
]

GROUP_WEIGHTS = {
    'Series': 10,
    'Title': 10, 
    'Chapter': 8,
    'Volume': 6,
    'Year': 3,
    'Extra': 2,
    'Source': 1,
    'Part': 1
}

# Quality indicators for pattern validation
SERIES_MIN_LENGTH = 2  # Minimum characters for a valid series name
MAX_REALISTIC_CHAPTER = 999  # Maximum realistic chapter number

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

def score_match(match_dict, filename):
    """Score a match based on weighted groups and quality checks"""
    if not match_dict:
        return 0
        
    score = 0

    # Reject matches with invalid captures
    if not validate_match(match_dict):
        return 0

    # Boost specific reliable patterns
    pattern_name = getattr(match_dict, '_pattern_name', None)

    if pattern_name == 'Series_Dash_Ch':
        score += 4
    elif pattern_name in ('Complete_Series', 'Complex_Series'):
        # Very common format with series name and chapter number
        if match_dict.get('Series') and match_dict.get('Chapter'):
            if 'Year' in match_dict and match_dict['Year']:  # Having a year is a good indicator
                score += 3

    # Add weighted scores for each group
    for key, value in match_dict.items():
        if value and isinstance(value, str) and value.strip():
            # Add the weight for this group
            score += GROUP_WEIGHTS.get(key, 1)
            
            # Additional quality checks
            if key in ('Series', 'Title'):
                # Higher score for longer series names (more specific matches)
                if len(value.strip()) >= SERIES_MIN_LENGTH:
                    score += min(len(value.strip()) / 10, 3)  # Up to 3 bonus points for long names
                else:
                    score -= 5  # Penalize very short series names
                
                # Penalize if the series name is just digits
                if value.strip().isdigit():
                    score -= 8
                
                # Penalize if series ends with "Ch." or "Chapter"
                if value.strip().endswith(("Ch.", "Ch", "Chapter")):
                    score -= 10
                
                # Strong penalty for ending with a separator
                if value.strip().endswith(("-", ":", ".")):
                    score -= 8 
                    
            elif key == 'Chapter':
                # Validate chapter numbers
                try:
                    ch_num = float(value.strip())
                    # Bonus for common chapter number formats
                    if 1 <= ch_num <= MAX_REALISTIC_CHAPTER:
                        score += 2
                    else:
                        # Penalize unrealistic chapter numbers
                        score -= 5
                except ValueError:
                    # Chapter not a clean number
                    if '-' in value:  # Range like "1-3" is okay
                        score += 1
                    else:
                        score -= 2
    
    # Ensure we have essential components (series name + chapter/volume)
    has_series = bool(match_dict.get('Series') or match_dict.get('Title'))
    has_numbering = bool(match_dict.get('Chapter') or match_dict.get('Volume'))
    
    if has_series and has_numbering:
        score += 5  # Bonus for having both elements
    elif not has_series:
        score -= 10  # Major penalty for missing series name
        
    # Check if the match covers a substantial part of the filename
    matched_parts = ''.join(str(v) for v in match_dict.values() if v)
    coverage_ratio = len(matched_parts) / len(filename)
    if coverage_ratio > 0.6:
        score += 3  # Good coverage of the filename
        
    # Print debug info for development
    # print(f"Score for {match_dict}: {score}")
    
    return score

def validate_match(match_dict):
    """Additional validation checks for a matched pattern"""
    # Check if there are invalid capture patterns
    for key, value in match_dict.items():
        if isinstance(value, str):
            # Extra field shouldn't start with a parenthesis
            if key == 'Extra' and value.startswith('('):
                return False
                
            # Series name shouldn't end with dash or other separators
            if key in ('Series', 'Title') and value.strip().endswith(('-', ':', '.', 'Ch.', 'Ch', 'Chapter')):
                return False
    
    return True

def match_best_pattern(filename, auto_mode=False):
    all_matches = []
    
    # Try all patterns and collect all matches with scores
    for pattern_name, pattern in patterns:
        match = pattern.match(filename)
        if match:
            match_dict = match.groupdict()
            # Store pattern name for scoring reference
            match_dict['_pattern_name'] = pattern_name
            score = score_match(match_dict, filename)
            all_matches.append((pattern_name, match_dict, score))
    
    # Sort matches by score in descending order
    all_matches.sort(key=lambda x: x[2], reverse=True)
    
    if not all_matches:
        return 'None', {}
    
    # Log top matches for debugging
    logger.debug(f"Top matches for '{filename}':")
    for i, (pattern_name, match_dict, score) in enumerate(all_matches[:3]):
        if i == 0 or score > 0:  # Only show positive scores beyond the top match
            logger.debug(f"  {pattern_name}: {match_dict} (Score: {score})")

    # In automatic mode, always select the best match if it's positive
    if auto_mode:
        if all_matches and all_matches[0][2] > 0:
            logger.info(f"Auto-selecting best match for '{filename}': {all_matches[0][0]} (Score: {all_matches[0][2]})")
            return all_matches[0][0], all_matches[0][1]
        else:
            logger.warning(f"No good matches found for '{filename}' in auto mode - skipping")
            return None
            
    # Interactive mode matches with close scores, ask user
    if len(all_matches) > 1 and all_matches[0][2] > 0 and all_matches[0][2] - all_matches[1][2] < 4:
        print(f"\nMultiple good matches found for: {filename}")
        for i, (pattern_name, match_dict, score) in enumerate(all_matches[:3]):  # Show top 3
            print(f"{i+1}. Pattern: {pattern_name} (Score: {score})")
            print(f"   Match: {match_dict}")
        
        choice = input(f"\nSelect pattern (1-{min(3, len(all_matches))}) or 'M' for manual entry: ").strip()
        
        if choice.lower() == 'm':
            manual_series = input("Enter series name: ").strip()
            manual_chapter = input("Enter chapter number (or press Enter to skip): ").strip()
            manual_volume = input("Enter volume number (or press Enter to skip): ").strip()
            
            match_dict = {'Series': manual_series}
            if manual_chapter:
                match_dict['Chapter'] = manual_chapter
            if manual_volume:
                match_dict['Volume'] = manual_volume
                
            return ("Manual Entry", match_dict)
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(all_matches):
                return all_matches[idx][0], all_matches[idx][1]
        except ValueError:
            pass
    
    # If best score is very low, suggest manual entry
    if all_matches and all_matches[0][2] < 5:
        print(f"\nLow confidence match for: {filename}")
        print(f"Best match: {all_matches[0][0]} (Score: {all_matches[0][2]})")
        print(f"Match data: {all_matches[0][1]}")
        
        choice = input("Accept this match? (Y/n/m for manual): ").strip().lower()
        
        if choice == 'n':
            return None
        elif choice == 'm':
            manual_series = input("Enter series name: ").strip()
            manual_chapter = input("Enter chapter number (or press Enter to skip): ").strip()
            manual_volume = input("Enter volume number (or press Enter to skip): ").strip()
            
            match_dict = {'Series': manual_series}
            if manual_chapter:
                match_dict['Chapter'] = manual_chapter
            if manual_volume:
                match_dict['Volume'] = manual_volume
                
            return ("Manual Entry", match_dict)
    
    # Return the best match if it exists and has a positive score
    if all_matches and all_matches[0][2] > 0:
        return all_matches[0][0], all_matches[0][1]
    
    # No good matches found
    print(f"\nNo good matches found for: {filename}")
    manual_choice = input("Enter 'M' to manually specify details, 'S' to skip this file: ").strip().lower()

    if manual_choice == 'm':
        manual_series = input("Enter series name: ").strip()
        manual_chapter = input("Enter chapter number (or press Enter to skip): ").strip()
        manual_volume = input("Enter volume number (or press Enter to skip): ").strip()
        
        match_dict = {'Series': manual_series}
        if manual_chapter:
            match_dict['Chapter'] = manual_chapter
        if manual_volume:
            match_dict['Volume'] = manual_volume
            
        return ("Manual Entry", match_dict)
    
    print(f"Skipping file: {filename}")
    return None

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

def process_directory(download_directory, library_path, dry_run=False, auto_mode=False):
    """Process manga files and move completed series folders to !Finished."""
    if dry_run:
        print("\n🔍 DRY RUN MODE - No files will be modified 🔍\n")
    
    if auto_mode:
        logger.info("Running in automatic mode - no user prompts will be shown")

    success = True
    processed_folders = set()
    
    # Create temp directory for processing
    temp_dir = os.path.join(download_directory, "!temp_processing")
    if not dry_run:
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
            if dry_run:
                print(f"\n📁 Found {len(rar_files)} RAR files in {root}")
                for rar_file in rar_files:
                    print(f"  📦 Would extract: {rar_file}")
                continue  # Skip actual extraction in dry run

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
                                match_type, match_dict = match_best_pattern(filename, auto_mode)
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
                match_type, match_dict = match_best_pattern(filename, auto_mode)
               
                if dry_run:
                    print(f"\n📄 Analyzing: {filename}")
                    print(f"  📋 Match type: {match_type}")
                    print(f"  📋 Match data: {match_dict}")
                    
                    if match_dict:
                        series = match_dict.get('Title') or match_dict.get('Series')
                        destination = os.path.join(library_path, series)
                        
                        file_name = series
                        if match_dict.get('Volume'):
                            file_name += f" v{match_dict.get('Volume')}"
                        if match_dict.get('Chapter'):
                            file_name += f" - Chapter {match_dict.get('Chapter')}"
                        file_name += ".cbz"  # Add the file extension
                        
                        dest_path = os.path.join(destination, file_name)
                        print(f"  ➡️  Would move to: {dest_path}")
                        
                        # Check for potential conflicts
                        if os.path.exists(destination):
                            if os.path.exists(dest_path):
                                print(f"  ⚠️  WARNING: File already exists at destination")
                    continue  # Skip actual processing in dry run
                
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
        if not dry_run and root != download_directory:
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
    if not dry_run and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    if dry_run:
        print("\n✅ Dry run completed. No files were modified.")

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
                    img.save(webp_path, 'WEBP', quality=75)
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
    file_name += os.path.splitext(source_file)[1]

    dest_path = os.path.join(series_path, file_name)

    # Check if source is directly in process directory
    source_dir = os.path.dirname(source_file)
    is_root_file = os.path.samefile(source_dir, os.path.dirname(os.path.abspath(source_file)))

    # Handle file already exists case
    if os.path.exists(dest_path):
        # Replace existing file with the (F) or (F1)-(F9) version
        if re.search(r'\(F\d?\)', os.path.basename(source_file)):
            os.remove(dest_path)
            shutil.move(source_file, dest_path)
            logger.warning(f"Overwriting {dest_path} with {source_file} as it is a fixed version")
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

def test_patterns(test_files):
    """Test pattern matching against sample filenames"""
    print("\n=== PATTERN TESTING ===")
    
    for filename in test_files:
        print(f"\nTesting: {filename}")
        
        # Try all patterns
        matches = []
        for pattern_name, pattern in patterns:
            match = pattern.match(filename)
            if match:
                match_dict = match.groupdict()
                score = score_match(match_dict, filename)
                matches.append((pattern_name, match_dict, score))
        
        # Sort by score
        matches.sort(key=lambda x: x[2], reverse=True)
        
        # Filter to show only positive scoring matches
        positive_matches = [m for m in matches if m[2] > 0]
        
        if not positive_matches:
            print("  ❌ No matches found")
        else:
            print(f"  ✓ Found {len(positive_matches)} positive matches:")
            for i, (pattern_name, match_dict, score) in enumerate(positive_matches):
                print(f"  {i+1}. {pattern_name} (Score: {score})")
                print(f"     {match_dict}")
            
            # Show example of how this would be processed
            if positive_matches:
                best_match = positive_matches[0]
                series = best_match[1].get('Title') or best_match[1].get('Series')
                volume = best_match[1].get('Volume')
                chapter = best_match[1].get('Chapter')
                
                print("\n  📝 Processing result would be:")
                print(f"     Series: {series}")
                if volume:
                    print(f"     Volume: {volume}")
                if chapter:
                    print(f"     Chapter: {chapter}")
    
    print("\n=== TESTING COMPLETE ===")

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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Manga Unpacker and Library Manager")
    parser.add_argument("--dry-run", "-d", action="store_true", 
                        help="Test pattern matching without moving files")
    parser.add_argument("--test", "-t", action="store_true",
                        help="Run pattern tests on sample filenames")
    parser.add_argument("--auto", "-a", action="store_true",
                        help="Run in automatic mode without user prompts (for scheduled jobs)")
    parser.add_argument("--source", "-s", type=str, metavar="DIR",
                        help="Source directory with raw downloads to process")
    parser.add_argument("--dest", "-l", type=str, metavar="DIR",
                        help="Destination library path for processed files")
    parser.add_argument("filenames", nargs="*", 
                        help="Optional filenames to test (only used with --test)")
    return parser.parse_args()

if __name__ == '__main__':
    logger = setup_logging()
    args = parse_arguments()

    if args.test:
        if args.filenames:
            # Use filenames provided on command line
            test_filenames = args.filenames
        else:
            # Enter interactive test mode
            print("\n=== PATTERN TESTING MODE ===")
            print("Enter filenames to test (one per line).")
            print("Leave blank and press Enter to finish.\n")
            
            test_filenames = []
            while True:
                filename = input("Enter filename to test (or press Enter to finish): ").strip()
                if not filename:
                    break
                test_filenames.append(filename)

        test_patterns(test_filenames)
        exit(0)

    library_path = args.dest if args.dest else get_library_path()
    download_directory = args.source if args.source else get_download_directory()

    process_directory(download_directory, library_path, dry_run=args.dry_run, auto_mode=args.auto)
