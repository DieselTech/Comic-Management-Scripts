"""
Manga Unpacker and Library Manager
=================================

A tool for organizing and processing manga/comic files by automatically detecting series names, 
chapter numbers, and volume information from filenames. This script can extract compressed files, 
convert images to WebP format for space savings, and organize files into a structured library.

Requirements:
------------
- Python 3.6+
- Dependencies: PIL (Pillow), rich, colorama, unrar (external command-line tool)
  Install with: pip install Pillow rich colorama

Usage:
-----
python Manga_Download_Processor.py [options]

Arguments:
---------
  --dry-run, -d       Test pattern matching without moving files
  --test-pattern, -tp Run pattern tests on sample filenames
  --auto, -a          Run in automatic mode without user prompts (for scheduled jobs)
  --source, -s DIR    Source directory with raw downloads to process
  --dest, -l DIR      Destination library path for processed files
  --work-dir, -w DIR  Work directory where processing occurs
  --mode, -m MODE     Processing mode: standard, bulk, nested, or auto (default)
  --threads, -t #     Maximum number of threads for image conversion (0=auto, default: 50% of CPU cores)
  
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

# Process using specific mode:
python Manga_Download_Processor.py --mode bulk --source /path/to/downloads --dest /path/to/library

User Interface:
-------------
The script features a modern, colorful interface with:
- Interactive prompts for configuration
- Visual progress indicators during conversion
- Clear summary information before processing
- Color-coded output for better readability
- Operation confirmation to prevent accidental actions

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

Folder Structure:
---------------
The script creates and manages these special folders:
- !temp_processing: Temporary folder for file conversion
- !temp_extract: Used for extracting compressed files
- !Finished: Completed series folders are moved here
- !Conflicts: Files that would overwrite existing ones

Library Organization:
------------------
Files will be organized as:
/library_path/
  /Series Name/
    Series Name v1 - Chapter 1.cbz
    Series Name v1 - Chapter 2.cbz
    ...

Automation:
---------
To run this script automatically on a schedule:

# Windows Task Scheduler example:
python c:/path/to/Manga_Download_Processor.py --auto --source "D:/Downloads/Manga" --dest "E:/Library"

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
from rich.progress import Progress, TextColumn, BarColumn, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn
from PIL import Image
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import init, Fore, Back, Style
import tempfile
import os.path
import uuid
import string
import random
import sys

init(autoreset=True)

patterns = [
    ('FullTitle_Year', re.compile(r'^(?P<Series>.+?)\s\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
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
    # Create logger with daily rotation and proper formatting
    logger = logging.getLogger('MangaUnpacker')
    logger.setLevel(logging.DEBUG)

    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(message)s')

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

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

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

    # Clean the filename for comparison purposes
    clean_filename = os.path.splitext(filename)[0]
    clean_filename = re.sub(r'\s+\(\d{4}\).*$', '', clean_filename)  # Remove year and anything after

    # Boost specific reliable patterns
    pattern_name = getattr(match_dict, '_pattern_name', None)

    if pattern_name == 'Series_Dash_Ch':
        score += 4
    elif pattern_name in ('Complete_Series', 'Complex_Series'):
        if match_dict.get('Series') and match_dict.get('Chapter'):
            if 'Year' in match_dict and match_dict['Year']:
                score += 3

    # Add weighted scores for each group
    for key, value in match_dict.items():
        if value and isinstance(value, str) and value.strip():
            score += GROUP_WEIGHTS.get(key, 1)
            
            # Quality checks for specific fields
            if key in ('Series', 'Title'):
                series_value = value.strip()

                if len(value.strip()) >= SERIES_MIN_LENGTH:
                    score += min(len(value.strip()) / 10, 3)
                else:
                    score -= 5
                
                if value.strip().isdigit():
                    score -= 8
                
                if value.strip().endswith(("Ch.", "Ch", "Chapter")):
                    score -= 10
                
                if value.strip().endswith(("-", ":", ".")):
                    score -= 8 
                
                # Check if series name is too short compared to filename
                if len(series_value) < len(clean_filename) * 0.3:  # Series less than 30% of filename length
                    score -= 8
                
                # Check for single word series from multi-word filename
                filename_words = len(re.findall(r'\b\w+\b', clean_filename))
                series_words = len(re.findall(r'\b\w+\b', series_value))
                if series_words == 1 and filename_words >= 3:
                    score -= 10
                
                # Check word overlap between filename and series
                if filename_words >= 3:  # Only check for multi-word titles
                    filename_word_set = set(w.lower() for w in re.findall(r'\b\w+\b', clean_filename))
                    series_word_set = set(w.lower() for w in re.findall(r'\b\w+\b', series_value))
                    common_words = filename_word_set.intersection(series_word_set)
                    if len(common_words) < len(filename_word_set) * 0.3:  # Less than 30% word overlap
                        score -= 8

            elif key == 'Chapter':
                try:
                    ch_num = float(value.strip())
                    if 1 <= ch_num <= MAX_REALISTIC_CHAPTER:
                        score += 2
                    else:
                        score -= 5
                except ValueError:
                    if '-' in value:
                        score += 1
                    else:
                        score -= 2
    
    # Check for essential components
    has_series = bool(match_dict.get('Series') or match_dict.get('Title'))
    has_numbering = bool(match_dict.get('Chapter') or match_dict.get('Volume'))
    
    if has_series and has_numbering:
        score += 5
    elif not has_series:
        score -= 10
        
    # Bonus for good filename coverage
    matched_parts = ''.join(str(v) for v in match_dict.values() if v)
    coverage_ratio = len(matched_parts) / len(filename)
    if coverage_ratio > 0.6:
        score += 3
    
    return score

def validate_match(match_dict):
    """Additional validation checks for a matched pattern"""
    for key, value in match_dict.items():
        if isinstance(value, str):
            if key == 'Extra' and value.startswith('('):
                return False
                
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

def extract_rars(folder_path, work_directory):
    """Extract RAR files from a single folder."""
    extracted_folders = []
    num_extracted = 0
    
    for file in os.listdir(folder_path):
        if file.endswith('.rar'):
            rar_path = os.path.join(folder_path, file)
            # Use the work directory for extraction
            folder_name = os.path.join(work_directory, "temp_extract", os.path.splitext(file)[0])
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

def process_directory(download_directory, library_path, work_directory, dry_run=False, auto_mode=False, process_mode="standard", max_threads=0):
    """Process manga files and move completed series folders to !Finished."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}🚀 PROCESSING COMICS")
    print(f"{Fore.CYAN}{'='*60}")
    
    if dry_run:
        print(f"\n{Fore.YELLOW}🔍 DRY RUN MODE - No files will be modified")
    
    if auto_mode:
        print(f"{Fore.BLUE}ℹ️ Running in automatic mode - no user prompts will be shown")

    print(f"{Fore.WHITE}Using processing mode: {Fore.YELLOW}{process_mode}")
    print(f"{Fore.WHITE}Using work directory: {Fore.YELLOW}{work_directory}")

    success = True
    processed_files_count = 0
    processed_folders_count = 0
    
    # Create necessary directories in work_directory instead of download_directory
    temp_processing_dir = os.path.join(work_directory, "temp_processing")
    temp_extract_dir = os.path.join(work_directory, "temp_extract")
    
    if not dry_run:
        # Create work directory structure
        os.makedirs(temp_processing_dir, exist_ok=True)
        os.makedirs(temp_extract_dir, exist_ok=True)
        print(f"{Fore.GREEN}✓ Created work directory structure")

    print(f"\n{Fore.CYAN}📂 SCANNING DIRECTORIES...")
    folder_count = 0
    
    # Create !Finished directory upfront
    finished_dir = os.path.join(download_directory, "!Finished")
    if not dry_run:
        os.makedirs(finished_dir, exist_ok=True)
    
    root_processed_files = []

    for root, _, files in os.walk(download_directory):
        if any(special in root for special in ["!Finished", "!temp_processing", "!temp_extract"]):
            continue
            
        cbz_files = [f for f in files if f.endswith('.cbz')]
        rar_files = [f for f in files if f.endswith('.rar')]
        
        if not cbz_files and not rar_files:
            continue
        
        folder_count += 1
        print(f"\n{Fore.WHITE}Processing folder {folder_count}: {Fore.YELLOW}{os.path.basename(root)}")
        print(f"{Fore.WHITE}  • Found: {Fore.GREEN}{len(cbz_files)} CBZ files, {Fore.GREEN}{len(rar_files)} RAR files")
        
        # Auto-detect appropriate processing mode based on content
        folder_process_mode = process_mode
        if process_mode == "auto":
            folder_process_mode = "bulk" if rar_files else "standard"
            print(f"{Fore.WHITE}  • Auto-detected mode: {Fore.YELLOW}{folder_process_mode}")
        
        processed_file_list = []
        if folder_process_mode == "bulk" and rar_files:
            process_result, processed_file_list = process_bulk_archives(root, files, work_directory, library_path, auto_mode, dry_run, max_threads)
        elif cbz_files:
            process_result, processed_file_list = process_individual_files(root, files, work_directory, library_path, auto_mode, dry_run, max_threads)
        else:
            print(f"{Fore.YELLOW}  ⚠ No processable files for mode {folder_process_mode}")
            process_result = False
            
        if processed_file_list:
            print(f"{Fore.GREEN}  ✓ Processed {len(processed_file_list)} files")
            processed_files_count += len(processed_file_list)
            
            if root == download_directory:
                root_processed_files.extend(processed_file_list)
        else:
            print(f"{Fore.YELLOW}  ⚠ No files were processed")
        
        # Move the folder to !Finished immediately if it was processed successfully
        # and it's not the main download directory
        if not dry_run and root != download_directory and process_result:
            folder_name = os.path.basename(root)
            dest_path = os.path.join(finished_dir, folder_name)
            try:
                print(f"{Fore.GREEN}  ✓ Moving folder to !Finished: {folder_name}")
                shutil.move(root, dest_path)
                processed_folders_count += 1
                logger.info(f"Moved processed folder to !Finished: {folder_name}")
            except Exception as e:
                print(f"{Fore.RED}  ✘ Error moving folder {folder_name}: {str(e)}")
                logger.error(f"Error moving folder {folder_name} to !Finished: {str(e)}")
        
    if not dry_run and root_processed_files:
        # No more nested subfolder - put files directly in !Finished
        os.makedirs(finished_dir, exist_ok=True)
        
        print(f"\n{Fore.CYAN}📦 Moving processed loose files")
        for filename in root_processed_files:
            source_path = os.path.join(download_directory, filename)
            if os.path.exists(source_path):
                try:
                    dest_path = os.path.join(finished_dir, filename)
                    shutil.move(source_path, dest_path)
                    print(f"{Fore.GREEN}  ✓ Moved to !Finished: {filename}")
                    logger.info(f"Moved processed loose file to !Finished: {filename}")
                except Exception as e:
                    print(f"{Fore.RED}  ✘ Error moving file {filename}: {str(e)}")
                    logger.error(f"Error moving loose file {filename}: {str(e)}")
    
    # Show summary but don't move folders (they're already moved)
    if not dry_run:
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.CYAN}📊 PROCESSING SUMMARY")
        print(f"{Fore.CYAN}{'='*60}")
        
        print(f"\n{Fore.WHITE}• Processed {Fore.GREEN}{processed_files_count} files")
        print(f"{Fore.WHITE}• Completed {Fore.GREEN}{processed_folders_count} folders")
    else:
        print(f"\n{Fore.YELLOW}📋 DRY RUN COMPLETED - No files were modified")

    if not dry_run:
    # Clean up main processing directories but keep work_directory itself
        for subdir in ["temp_processing", "temp_extract"]:
            path = os.path.join(work_directory, subdir)
            if os.path.exists(path):
                shutil.rmtree(path)
        print(f"{Fore.GREEN}✓ Cleaned up temporary files")

    print(f"\n{Fore.GREEN}{'='*60}")
    print(f"{Fore.GREEN}✅ PROCESSING COMPLETE")
    print(f"{Fore.GREEN}{'='*60}")
    
    return success

def process_individual_files(root, files, work_directory, library_path, auto_mode, dry_run, max_threads=0):
    """Process individual CBZ files directly"""
    cbz_files = [f for f in files if f.endswith('.cbz')]
    
    if not cbz_files:
        return False, []
    
    if dry_run:
        print(f"{Fore.WHITE}  • Would process {Fore.YELLOW}{len(cbz_files)} CBZ files {Fore.WHITE}(standard mode)")
        for cbz_file in cbz_files:
            filename = cbz_file
            try:
                match_type, match_dict = match_best_pattern(filename, auto_mode)
                print(f"\n{Fore.WHITE}    📄 Analyzing: {Fore.YELLOW}{filename}")
                print(f"{Fore.WHITE}    📋 Match type: {Fore.CYAN}{match_type}")
                
                if match_dict:
                    series = match_dict.get('Title') or match_dict.get('Series')
                    destination = os.path.join(library_path, series)
                    
                    file_name = series
                    if match_dict.get('Volume'):
                        file_name += f" v{match_dict.get('Volume')}"
                    if match_dict.get('Chapter'):
                        file_name += f" - Chapter {match_dict.get('Chapter')}"
                    file_name += ".cbz"
                    
                    dest_path = os.path.join(destination, file_name)
                    print(f"{Fore.WHITE}    ➡️  Would move to: {Fore.GREEN}{dest_path}")
                    
                    if os.path.exists(destination) and os.path.exists(dest_path):
                        print(f"{Fore.YELLOW}    ⚠️  File already exists at destination")
            except Exception as e:
                print(f"{Fore.RED}    ❌ Error analyzing {filename}: {str(e)}")
        return True, []
    
    success = True
    files_processed = False
    processed_files = []

    finished_dir = os.path.join(os.path.dirname(root), "!Finished")
    
    for file in cbz_files:
        filepath = os.path.join(root, file)
        filename = os.path.basename(filepath)
        try:
            match_result = match_best_pattern(filename, auto_mode)
            if match_result is None:
                print(f"{Fore.YELLOW}    ⚠ Skipping: {filename} (no match)")
                continue
                
            match_type, match_dict = match_result
            
            print(f"{Fore.WHITE}    📄 Processing: {Fore.YELLOW}{filename}")
            
            if not match_dict or (not match_dict.get('Title') and not match_dict.get('Series')):
                print(f"{Fore.RED}    ❌ Could not determine series")
                continue

            # Process the CBZ file
            result = process_cbz_file(filepath, work_directory, library_path, match_dict, max_threads)
            if result:
                files_processed = True
                processed_files.append(file)
                print(f"{Fore.GREEN}    ✓ Successfully processed")
                
                # If this is a root directory file, move it to !Finished immediately
                if root == download_directory and not dry_run:
                    finished_dir = os.path.join(download_directory, "!Finished")
                    os.makedirs(finished_dir, exist_ok=True)
                    dest_path = os.path.join(finished_dir, filename)
                    try:
                        shutil.move(filepath, dest_path)
                        print(f"{Fore.GREEN}    ✓ Moved to !Finished: {filename}")
                        logger.info(f"Moved processed loose file to !Finished: {filename}")
                    except Exception as e:
                        print(f"{Fore.RED}    ✘ Error moving file {filename}: {str(e)}")
                        logger.error(f"Error moving loose file {filename}: {str(e)}")
            else:
                print(f"{Fore.RED}    ✘ Processing failed")
            success = success and result
        except Exception as e:
            print(f"{Fore.RED}    ❌ Error processing {filename}: {str(e)}")
            success = False
    
    return success and files_processed, processed_files

def process_bulk_archives(root, files, work_directory, library_path, auto_mode, dry_run, max_threads=0):
    """Process RAR files containing multiple CBZs"""
    rar_files = [f for f in files if f.endswith('.rar')]
    
    if not rar_files:
        logger.info("No RAR files found to process in bulk mode")
        return False, []  # No RAR files to process, not successful
    
    if dry_run:
        print(f"\n📁 Found {len(rar_files)} RAR files in {root} (bulk mode)")
        for rar_file in rar_files:
            print(f"  📦 Would extract: {rar_file}")
        return True, []
    
    success = True
    files_processed = False
    # Fix: Pass the work_directory parameter to extract_rars
    extracted_folders, num_extracted = extract_rars(root, work_directory)
    
    if num_extracted > 0:
        # Process extracted files
        for extract_dir in extracted_folders:
            extracted_cbz_files = [f for f in os.listdir(extract_dir) if f.endswith('.cbz')]
            logger.info(f"Found {len(extracted_cbz_files)} CBZ files in extracted folder: {extract_dir}")
            
            if not extracted_cbz_files:
                logger.warning(f"No CBZ files found in extracted folder: {extract_dir}")
                continue
                
            files_processed = True
            for extracted_file in extracted_cbz_files:
                filepath = os.path.join(extract_dir, extracted_file)
                filename = os.path.basename(filepath)
                try:
                    # Process the extracted CBZ
                    match_result = match_best_pattern(filename, auto_mode)
                    if match_result is None:
                        logger.warning(f"Skipping file with no match: {filename}")
                        continue
                        
                    match_type, match_dict = match_result
                    logger.info(f"\nProcessing extracted: {filename}")
                    logger.info(f"Match Type: {match_type}")
                    logger.info(f"Match: {match_dict}")

                    if not match_dict or (not match_dict.get('Title') and not match_dict.get('Series')):
                        logger.warning(f"Could not determine series for {filename}")
                        continue

                    # Process extracted CBZ file
                    result = process_cbz_file(filepath, work_directory, library_path, match_dict, max_threads)
                    success = success and result
                except Exception as e:
                    logger.error(f"Error processing extracted file {filepath}: {str(e)}")
                    success = False
    else:
        logger.warning("No files extracted from RAR archives")
        success = False
    
    # Clean up extraction folders
    processed_files = []
    if not dry_run:
        for file in rar_files:
            if success and files_processed:
                processed_files.append(file)

    return success and files_processed, processed_files

def convert_to_webp(source_filepath, temp_dir, max_threads=0):
    """Convert images in a CBZ file to WebP format and create a new CBZ."""
    try:
        with zipfile.ZipFile(source_filepath, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        new_filepath = os.path.join(temp_dir, os.path.basename(source_filepath).replace('.cbz', '_webp.cbz'))
        with zipfile.ZipFile(new_filepath, 'w') as new_zip:
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
            
            if max_threads <= 0:
                # Default to 50% of available cores (but at least 1)
                import multiprocessing
                cpu_count = multiprocessing.cpu_count()
                max_workers = max(1, cpu_count // 2)
                logger.info(f"Auto-configured thread count: {max_workers} (50% of {cpu_count} cores)")
            else:
                max_workers = max_threads
                logger.info(f"Using user-specified thread count: {max_workers}")

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold cyan]{task.description}"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn()
            ) as progress:
                convert_task = progress.add_task("Converting images to WebP", total=len(image_files))
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(convert_image, img_path) for img_path in image_files]
                    for future in as_completed(futures):
                        webp_path = future.result()
                        new_zip.write(webp_path, os.path.basename(webp_path))
                        progress.update(convert_task, advance=1)
                    
        return new_filepath
    except Exception as e:
        logger.error(f"Error converting to WebP: {str(e)}")
        return None

def process_cbz_file(filepath, work_dir, library_path, match_dict, max_threads=0):
    """Process a single CBZ file with isolated temp directory."""
    try:
        # Create a unique subfolder for this file
        file_id = os.path.splitext(os.path.basename(filepath))[0]
        file_temp_dir = os.path.join(work_dir, "temp_processing", sanitize_filename(file_id))
        
        # Clean any existing directory with the same name
        if os.path.exists(file_temp_dir):
            shutil.rmtree(file_temp_dir)
        os.makedirs(file_temp_dir, exist_ok=True)
        
        original_size = os.path.getsize(filepath)
        print(f"{Fore.WHITE}      • Original size: {Fore.YELLOW}{original_size / (1024*1024):.2f} MB")

        print(f"{Fore.WHITE}      • Converting to WebP...")
        new_filepath = convert_to_webp(filepath, file_temp_dir, max_threads)
        
        if not new_filepath or not os.path.exists(new_filepath):
            print(f"{Fore.YELLOW}      ⚠ WebP conversion failed, using original file")
            # Copy original to temp dir to ensure consistent behavior
            file_to_move = os.path.join(file_temp_dir, os.path.basename(filepath))
            shutil.copy2(filepath, file_to_move)
            logger.info(f"Copied original file to temp directory due to WebP conversion failure")
        else:
            new_size = os.path.getsize(new_filepath)
            print(f"{Fore.WHITE}      • WebP size: {Fore.YELLOW}{new_size / (1024*1024):.2f} MB")
            
            if new_size < original_size:
                size_reduction = ((original_size - new_size) / original_size) * 100
                print(f"{Fore.GREEN}      ✓ Size reduced by {size_reduction:.1f}%")
                file_to_move = new_filepath
            else:
                print(f"{Fore.YELLOW}      ⚠ WebP increased file size, using original")
                # Copy original to temp dir for consistent behavior
                file_to_move = os.path.join(file_temp_dir, os.path.basename(filepath))
                shutil.copy2(filepath, file_to_move)
                logger.info(f"Copied original file to temp directory due to WebP size increase")

        series = match_dict.get('Title') or match_dict.get('Series')
        volume = match_dict.get('Volume')
        chapter = match_dict.get('Chapter')
        
        dest_info = f"{series}"
        if volume:
            dest_info += f" v{volume}"
        if chapter:
            dest_info += f" - Chapter {chapter}"
            
        print(f"{Fore.WHITE}      • Moving to library: {Fore.GREEN}{dest_info}")

        move_to_library(file_to_move, library_path, series, volume, chapter)
        print(f"{Fore.GREEN}      ✓ File moved successfully")

        # Clean up this file's temp directory completely
        if os.path.exists(file_temp_dir):
            shutil.rmtree(file_temp_dir)
            logger.debug(f"Cleaned up temporary directory: {file_temp_dir}")

        return True
    except Exception as e:
        print(f"{Fore.RED}      ❌ Error: {str(e)}")
        logger.error(f"Error processing file {filepath}: {str(e)}", exc_info=True)
        # Make sure to clean up even on error
        try:
            if os.path.exists(file_temp_dir):
                shutil.rmtree(file_temp_dir)
        except Exception as cleanup_error:
            logger.error(f"Cleanup error: {str(cleanup_error)}")
        return False

def sanitize_filename(filename):
    """Convert filename to a safe version for use as a directory name."""
    # Replace problematic characters with underscores
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def series_exists(library_path, series_name):
    series_path = os.path.join(library_path, series_name)
    return os.path.exists(series_path)

def move_to_library(source_file, library_path, series_name, volume=None, chapter=None, download_directory=None):
    series_path = os.path.join(library_path, series_name)

    if not os.path.exists(series_path):
        os.makedirs(series_path)

    file_name = series_name
    if volume:
        file_name += f" v{volume}"
    if chapter:
        file_name += f" - Chapter {chapter}"
    file_name += os.path.splitext(source_file)[1]

    dest_path = os.path.join(series_path, file_name)

    # Check if source is directly in process directory (affects copy vs move behavior)
    source_dir = os.path.dirname(source_file)
    is_root_file = os.path.samefile(source_dir, os.path.dirname(os.path.abspath(source_file)))

    if os.path.exists(dest_path):
        # Handle file already exists cases
        if re.search(r'\(F\d?\)', os.path.basename(source_file)):
            os.remove(dest_path)
            shutil.move(source_file, dest_path)
            logger.warning(f"Overwriting {dest_path} with {source_file} as it is a fixed version")
        elif download_directory:  # Only use if provided
            # Move to conflicts folder with unique name
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
        else:
            base, ext = os.path.splitext(dest_path)
            counter = 1
            while os.path.exists(dest_path):
                dest_path = f"{base}_{counter}{ext}"
                counter += 1
            logger.warning(f"Conflict: File already exists. Saving as: {dest_path}")
    
    # Copy from root directory, move from subdirectories
    if is_root_file:
        shutil.copy2(source_file, dest_path)
        logger.info(f"Copied {source_file} to {dest_path}")
    else:
        shutil.move(source_file, dest_path)
        logger.info(f"Moved {source_file} to {dest_path}")

def test_patterns(test_files):
    """Test pattern matching against sample filenames"""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}🧪 PATTERN TESTING")
    print(f"{Fore.CYAN}{'='*60}")
    
    for i, filename in enumerate(test_files, 1):
        print(f"\n{Fore.WHITE}Testing file {i}/{len(test_files)}: {Fore.YELLOW}{filename}")
        
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
            print(f"{Fore.RED}  ❌ No matches found")
        else:
            print(f"{Fore.GREEN}  ✓ Found {len(positive_matches)} positive matches:")
            for i, (pattern_name, match_dict, score) in enumerate(positive_matches):
                print(f"  {i+1}. {Fore.BLUE}{pattern_name} {Fore.WHITE}(Score: {Fore.GREEN}{score}{Fore.WHITE})")
                for key, value in match_dict.items():
                    if key != '_pattern_name':
                        print(f"     {Fore.YELLOW}{key}: {Fore.WHITE}{value}")
            
            # Show example of how this would be processed
            if positive_matches:
                best_match = positive_matches[0]
                series = best_match[1].get('Title') or best_match[1].get('Series')
                volume = best_match[1].get('Volume')
                chapter = best_match[1].get('Chapter')
                
                print(f"\n  {Fore.CYAN}📝 Processing result would be:")
                print(f"     {Fore.WHITE}Series: {Fore.GREEN}{series}")
                if volume:
                    print(f"     {Fore.WHITE}Volume: {Fore.GREEN}{volume}")
                if chapter:
                    print(f"     {Fore.WHITE}Chapter: {Fore.GREEN}{chapter}")
    
    print(f"\n{Fore.GREEN}{'='*60}")
    print(f"{Fore.GREEN}✅ TESTING COMPLETE")
    print(f"{Fore.GREEN}{'='*60}")

def clean_work_directory(work_directory, force=False):
    """Clean up the work directory by removing the entire random directory."""
    try:
        if os.path.exists(work_directory):
            # Safety check: Make sure it's a random directory we created
            if os.path.basename(work_directory).startswith("mproc_"):
                # Clean the entire directory
                shutil.rmtree(work_directory)
                print(f"{Fore.GREEN}✓ Cleaned up work directory: {work_directory}")
                return True
            else:
                # This is not our random directory
                print(f"{Fore.RED}✘ Safety check failed: {work_directory} doesn't appear to be a temporary processing directory")
                print(f"{Fore.RED}  Directory name should start with 'mproc_'")
                return False
        else:
            print(f"{Fore.YELLOW}⚠ Work directory does not exist: {work_directory}")
            return True
    except Exception as e:
        print(f"{Fore.RED}✘ Error cleaning work directory: {str(e)}")
        return False

def generate_random_dirname(length=8):
    """Generate a random alphanumeric directory name."""
    chars = string.ascii_letters + string.digits
    return "mproc_" + ''.join(random.choice(chars) for _ in range(length))

def get_work_directory():
    """Get base work directory and create a random subdirectory within it."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}🔧 TEMPORARY WORK DIRECTORY")
    print(f"{Fore.CYAN}{'='*60}")
    
    print(f"\n{Fore.WHITE}This is where temporary processing files will be stored.")
    print(f"{Fore.WHITE}The script will create a random subdirectory in your chosen location.")
    print(f"{Fore.WHITE}This directory should have plenty of free space (at least 10GB recommended).")
    
    # Try to use system temp by default
    default_dir = tempfile.gettempdir()
    
    print(f"\n{Fore.YELLOW}Default: {default_dir}")
    print(f"{Fore.YELLOW}Examples: ")
    print(f"{Fore.YELLOW}  • D:\\TempWork")
    print(f"{Fore.YELLOW}  • C:\\Temp")
    
    while True:
        print()
        use_default = input(f"{Fore.GREEN}➤ Use default location? (y/n): {Fore.WHITE}").strip().lower()
        
        if use_default == 'y':
            base_dir = default_dir
        else:
            base_dir = input(f"{Fore.GREEN}➤ Enter base work directory path: {Fore.WHITE}")
        
        if not base_dir.strip():
            print(f"{Fore.RED}✘ Path cannot be empty. Please enter a valid directory.")
            continue
            
        if not os.path.exists(base_dir):
            print(f"{Fore.YELLOW}Directory '{base_dir}' doesn't exist.")
            create_option = input(f"{Fore.GREEN}➤ Create this directory? (y/n): {Fore.WHITE}")
            
            if create_option.lower() != 'y':
                continue
                
            try:
                os.makedirs(base_dir)
                print(f"{Fore.GREEN}✓ Directory created successfully!")
            except Exception as e:
                print(f"{Fore.RED}✘ Error creating directory: {str(e)}")
                continue
                
        # Create a random subdirectory for this processing run
        random_dirname = generate_random_dirname()
        work_dir = os.path.join(base_dir, random_dirname)
        
        try:
            os.makedirs(work_dir)
            print(f"{Fore.GREEN}✓ Created temporary work directory: {work_dir}")
            return work_dir
        except Exception as e:
            print(f"{Fore.RED}✘ Error creating work directory: {str(e)}")

def get_library_path():
    """Get and validate the comic library path with enhanced UI."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}📚 COMIC LIBRARY LOCATION")
    print(f"{Fore.CYAN}{'='*60}")
    
    print(f"\n{Fore.WHITE}This is where your organized comics will be stored.")
    print(f"{Fore.WHITE}Each series will get its own folder within this location.")
    
    print(f"\n{Fore.YELLOW}Examples: ")
    print(f"{Fore.YELLOW}  • C:\\Comics\\Library")
    print(f"{Fore.YELLOW}  • D:\\Media\\Comics")
    
    while True:
        print()
        library_path = input(f"{Fore.GREEN}➤ Enter your comic library path: {Fore.WHITE}")
        
        if not library_path.strip():
            print(f"{Fore.RED}✘ Path cannot be empty. Please enter a valid directory.")
            continue
            
        if os.path.exists(library_path):
            print(f"{Fore.GREEN}✓ Path verified!")
            return library_path
            
        print(f"{Fore.RED}✘ Path '{library_path}' doesn't exist.")
        create_option = input(f"{Fore.YELLOW}Would you like to create this directory? (y/n): {Fore.WHITE}")
        
        if create_option.lower() == 'y':
            try:
                os.makedirs(library_path)
                print(f"{Fore.GREEN}✓ Directory created successfully!")
                return library_path
            except Exception as e:
                print(f"{Fore.RED}✘ Error creating directory: {str(e)}")

def get_download_directory():
    """Get and validate the download directory with enhanced UI."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}📥 DOWNLOADS LOCATION")
    print(f"{Fore.CYAN}{'='*60}")
    
    print(f"\n{Fore.WHITE}This is where your unprocessed comic downloads are located.")
    print(f"{Fore.WHITE}The script will search this folder and all its subfolders for CBZ/RAR files.")
    
    print(f"\n{Fore.YELLOW}Examples: ")
    print(f"{Fore.YELLOW}  • C:\\Downloads\\Comics")
    print(f"{Fore.YELLOW}  • D:\\Torrents\\Completed")
    
    while True:
        print()
        download_directory = input(f"{Fore.GREEN}➤ Enter the directory with raw downloads to process: {Fore.WHITE}")
        
        if not download_directory.strip():
            print(f"{Fore.RED}✘ Path cannot be empty. Please enter a valid directory.")
            continue
            
        if os.path.exists(download_directory):
            # Count files in root directory
            root_files = sum(1 for f in os.listdir(download_directory) 
                         if os.path.isfile(os.path.join(download_directory, f)) 
                         and f.lower().endswith(('.cbz', '.rar')))
            
            # Count files in all subdirectories
            subdir_files = 0
            for root, _, files in os.walk(download_directory):
                if any(special in root for special in ["!Finished", "!temp_processing", "!temp_extract"]):
                    continue
                subdir_files += sum(1 for f in files if f.lower().endswith(('.cbz', '.rar')))
            
            # Subtract root files to avoid double-counting
            subdir_files -= root_files
            
            print(f"{Fore.GREEN}✓ Path verified! Found:")
            print(f"{Fore.GREEN}  • {root_files} files in root directory")
            print(f"{Fore.GREEN}  • {subdir_files} files in subdirectories")
            
            if root_files == 0 and subdir_files == 0:
                print(f"{Fore.YELLOW}⚠ Warning: No CBZ or RAR files found in this location or its subdirectories.")
                proceed = input(f"{Fore.YELLOW}Continue anyway? (y/n): {Fore.WHITE}").strip().lower()
                if proceed != 'y':
                    continue
            
            return download_directory
            
        print(f"{Fore.RED}✘ Directory '{download_directory}' doesn't exist.")

def confirm_processing(download_directory, library_path, work_directory, dry_run=False, process_mode="auto"):
    """Show a summary of the operation and ask for confirmation before proceeding."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}💼 OPERATION SUMMARY")
    print(f"{Fore.CYAN}{'='*60}")
    
    # Count files to be processed
    root_files = sum(1 for f in os.listdir(download_directory) 
                  if os.path.isfile(os.path.join(download_directory, f)) 
                  and f.lower().endswith(('.cbz', '.rar')))
    
    # Count files in all subdirectories
    subdir_files = 0
    for root, _, files in os.walk(download_directory):
        if any(special in root for special in ["!Finished", "!temp_processing", "!temp_extract"]):
            continue
        subdir_files += sum(1 for f in files if f.lower().endswith(('.cbz', '.rar')))
    
    # Subtract root files to avoid double-counting
    subdir_files -= root_files
    
    # Determine the operation mode description
    mode_desc = {
        "auto": "Automatic (detect best method based on content)",
        "standard": "Standard (process individual CBZ files)",
        "bulk": "Bulk (extract and process RAR archives)",
        "nested": "Nested (process folder hierarchies)"
    }.get(process_mode, "Unknown")
    
    print(f"\n{Fore.WHITE}The script will now:")
    print(f"{Fore.WHITE}1. Search for comic files in {Fore.YELLOW}{download_directory}")
    print(f"{Fore.WHITE}2. Process {Fore.YELLOW}{root_files + subdir_files} files {Fore.WHITE}({root_files} in root, {subdir_files} in subfolders)")
    print(f"{Fore.WHITE}3. Organize them into {Fore.YELLOW}{library_path}")
    print(f"{Fore.WHITE}4. Use processing mode: {Fore.YELLOW}{mode_desc}")
    
    if dry_run:
        print(f"\n{Fore.YELLOW}⚠ DRY RUN MODE: No files will be modified")
    
    print(f"\n{Fore.WHITE}During processing:")
    print(f"{Fore.WHITE}• Files will be converted to WebP format when beneficial")
    print(f"{Fore.WHITE}• Series folders will be created automatically")
    print(f"{Fore.WHITE}• Processed folders will move to !Finished directory")
    
    while True:
        proceed = input(f"\n{Fore.GREEN}➤ Continue with processing? (y/n): {Fore.WHITE}").strip().lower()
        if proceed == 'y':
            return True
        elif proceed == 'n':
            print(f"{Fore.YELLOW}Operation canceled by user.")
            return False
        else:
            print(f"{Fore.RED}Please enter 'y' to continue or 'n' to cancel.")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Manga Unpacker and Library Manager")
    parser.add_argument("--dry-run", "-d", action="store_true", 
                        help="Test pattern matching without moving files")
    parser.add_argument("--test-pattern", "-tp", action="store_true",
                        help="Run pattern tests on sample filenames")
    parser.add_argument("--auto", "-a", action="store_true",
                        help="Run in automatic mode without user prompts (for scheduled jobs)")
    parser.add_argument("--source", "-s", type=str, metavar="DIR",
                        help="Source directory with raw downloads to process")
    parser.add_argument("--dest", "-l", type=str, metavar="DIR",
                        help="Destination library path for processed files")
    parser.add_argument("filenames", nargs="*", 
                        help="Optional filenames to test (only used with --test-pattern)")
    parser.add_argument("--mode", "-m", type=str, choices=["standard", "bulk", "nested", "auto"], default="auto",
                        help="Processing mode: standard (individual files), bulk (archives containing multiple CBZs), "
                             "nested (folders of archives), auto (detect based on content)")
    parser.add_argument("--work-dir", "-w", type=str, metavar="DIR",
                        help="Work directory for temporary processing (with plenty of free space)")
    parser.add_argument("--threads", "-t", type=int, default=0, 
                        help="Maximum number of threads for image conversion (0=auto, default: 50% of CPU cores)")
    return parser.parse_args()


def check_dependencies():
    """Check for required dependencies and libraries"""
    missing_deps = []
    
    # Check unrar
    unrar_path = shutil.which("unrar")
    if unrar_path is None:
        error_msg = "ERROR: 'unrar' is not installed or not found in the system path."
        missing_deps.append(error_msg)
        print(f"{Fore.RED}✘ {error_msg}", file=sys.stderr)
        
    # Check Python libraries
    required_libs = [
        ("zipfile", "Standard library"),
        ("PIL.Image", "Pillow"),
        ("concurrent.futures", "Standard library"),
        ("rich.progress", "rich")
    ]
    
    for lib, package in required_libs:
        try:
            __import__(lib)
        except ImportError:
            error_msg = f"ERROR: Missing required library: {lib} (from package '{package}')"
            missing_deps.append(error_msg)
            print(f"{Fore.RED}✘ {error_msg}", file=sys.stderr)
    
    # If any deps are missing, write to log file and exit
    if missing_deps:
        try:
            # Write plain text version to a dedicated file for service managers
            with open("dependency_check_failed.log", "w") as f:
                f.write("Comic Manager dependency check failed\n")
                f.write("=====================================\n")
                f.write("\n".join(missing_deps))
                f.write("\n\nPlease install required dependencies and try again.")
            
            # Also log to the regular log file
            logger.error("Dependency check failed: " + ", ".join(missing_deps))
        except Exception as e:
            print(f"Failed to write dependency error log: {e}", file=sys.stderr)
            
        print(f"{Fore.YELLOW}Please install the required libraries and try again.", file=sys.stderr)
        exit(1)
    else:
        print(f"{Fore.GREEN}✓ All dependencies are installed.")


if __name__ == '__main__':
    logger = setup_logging()
    args = parse_arguments()
    
    # Dependency check at startup
    check_dependencies()


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
    work_directory = args.work_dir if args.work_dir else get_work_directory()

    if not args.auto:
        if not confirm_processing(download_directory, library_path, work_directory,args.dry_run, args.mode):
            print(f"{Fore.YELLOW}Exiting without processing.")
            exit(0)

    process_directory(download_directory, library_path, work_directory, dry_run=args.dry_run, auto_mode=args.auto, process_mode=args.mode, max_threads=args.threads)
