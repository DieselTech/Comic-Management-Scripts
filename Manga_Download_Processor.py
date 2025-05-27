"""
Manga Unpacker and Library Manager
=================================

A tool for organizing and processing manga/comic files by automatically detecting series names, 
chapter numbers, and volume information from filenames. This script can extract compressed files, 
convert images to WebP format for space savings, and organize files into a structured library.

The envisioned workflow is to run with --dry-run to test the pattern matching so that it can be refined before actual processing.
The --dry-run mode will still record the pattern matches in the database, but it won't move or convert any files.
Once you have the matching patterns set up, you can run the script with --auto to process files automatically.
Otherwise, running in interactive mode will also allow you to refine the pattern matches as you go.

Version: 0.9.7.1
Updated: 5/27/2025

Features:
--------
- Advanced pattern matching to detect series information from filenames
- Automatic conversion to WebP format for significant space savings
- Persistent pattern database that remembers your processing choices
- Interactive and fully automatic processing modes
- Support for individual files, bulk archives, and nested folder structures
- Visual progress tracking with detailed statistics
- Ability to undo previous processing runs

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
  --threads, -t #     Maximum number of threads for image conversion (default is 50% of cores)
  --db-stats          Show statistics about the pattern database
  --test-db           Test database matches against files without processing them
  --undo              Undo a previous processing run
  --clear-patterns    Clear all stored pattern matches from the database
  
Examples:
--------
# Interactive mode (prompted for paths):
python Manga_Download_Processor.py

# Dry run to preview what would happen:
python Manga_Download_Processor.py --dry-run --source /path/to/downloads --dest /path/to/library

# Test pattern matching on specific filenames:
python Manga_Download_Processor.py --test-pattern "My Series c10 (2023).cbz" "Another Example v02 (2022).cbz"

# Fully automated mode for scheduled jobs:
python Manga_Download_Processor.py --auto --source /path/to/downloads --dest /path/to/library

# Process using specific mode:
python Manga_Download_Processor.py --mode bulk --source /path/to/downloads --dest /path/to/library

# View database statistics:
python Manga_Download_Processor.py --db-stats

# Undo a previous processing run:
python Manga_Download_Processor.py --undo

Library Organization:
------------------
Files will be organized as:
/library_path/
  /Series Name/
    Series Name v1.cbz
    Series Name Chapter 20.cbz
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
import sqlite3
import os.path
import time
from pathlib import Path

init(autoreset=True)
can_process_rar = True
_stored_pattern_choice = None
_stored_manual_pattern = None
_stored_skip_series = None

patterns = [
    ('FullTitle_Year', re.compile(r'^(?P<Series>.+?)\s\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Series_Oneshot_Year', re.compile(r'^(?P<Series>.+?)\s+-\s+One-shot\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Series_c_Chapter_Year', re.compile(r'^(?P<Series>.+?)\sc(?P<Chapter>\d+(?:\.\d+)?)\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Complete_Series', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d+(?:\.\d+)?)\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Series_Chapter_RepeatNumber', re.compile(r'^(?P<Series>.+?)\s(?P<Chapter>\d+)\s+-\s+\w+\s+\d+\s+\((?P<Year>\d{4})\)')),
    ('Complex_Series', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3})\s+\((?P<Year>\d{4})\)')),
    ('Complex_SeriesDecimal', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3}(?:\.\d+)?)\s+\((?P<Year>\d{4})\)')),
    ('Complex_Series2', re.compile(r'(?P<Series>.+?)\s(?P<Chapter>\d{3})(?:\s+-\s+.+?)?\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Series_With_Dash_Chapter_Subtitle', re.compile(r'^(?P<Series>.+?)\s(?P<Chapter>\d{3})\s+-\s+(?P<Extra>.+?)\s+\((?P<Year>\d{4})\)')),
    ('Webtoon_Season_Bracket', re.compile(r'^(?P<Series>.+?)\s\[Season\s(?P<Volume>\d+)\]\s+Ep\.\s+(?P<Chapter>\d+(?:\.\d+|-\d+)?)')),
    ('Webtoon_Season', re.compile(r'^(?P<Series>.+?)\s+Season\s+(?P<Volume>\d+)\s+Ep\.\s+(?P<Chapter>\d+(?:\.\d+|-\d+)?)')),
    ('Webtoon_S_c', re.compile(r'^(?P<Series>.+?)\s+S(?P<Volume>\d+)\s+c(?P<Chapter>\d+(?:\.\d+|-\d+)?)')),
    ('Webtoon_Season_Epilogue', re.compile(r'^(?P<Series>.+?)\s+Season\s+(?P<Volume>\d+)\s+\((?P<Extra>[^)]+)\)')),
    ('Webtoon_Season_Parentheses', re.compile(r'^(?P<Series>.+?)\s+\(S(?P<Volume>\d+)\)\s+Episode\s+(?P<Chapter>\d+(?:\.\d+|-\d+)?)')),
    ('Webtoon_Episode', re.compile(r'^(?P<Series>.+?)\s+Episode\s+(?P<Chapter>\d+(?:\.\d+|-\d+)?)')),
    ('Webtoon_Bracket_Vol_Ch', re.compile(r'^(?P<Series>.+?)\s\[(?:vol|volume)\s+(?P<Volume>\d+)\]\s+(?:ch|chapter)\.?\s+(?P<Chapter>\d+(?:\.\d+|-\d+)?)', re.IGNORECASE)),
    ('Webtoon_Hash_Ep', re.compile(r'^(?P<Series>.+?)\s+#(?P<Chapter>\d+)(?:\s+-\s+Ep\.\s+\d+)?(?:\.cbz|\.cbr)?$')),
    ('Series_Vol_Bare_Chapter_Space', re.compile(r'^(?P<Series>.+?)\s+Vol\.\s+(?P<Volume>\d+)\s+(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Series_Vol_Bare_Chapter', re.compile(r'^(?P<Series>.+?)(?:\s+Vol\.(?P<Volume>\d+))(?:\s+)(?P<Chapter>\d+(?:\.\d+|-\d+)?)')),
    ('Series_Vol_Ch', re.compile(r'^(?P<Series>.+?)\s+Vol\.?\s*(?P<Volume>\d+)\s+Ch\.?\s*(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Ch', re.compile(r'(\b|_)(c|ch)(\.?\s?)(?P<Chapter>(\d+(\.\d)?)(-c?\d+(\.\d)?)?)')),
    ('Ch_bare', re.compile(r'^(?!.*\b(?:v|vo|vol|Volume)\b)(?P<Series>.+?)(\s|_)(?P<Chapter>\d+(?:\.\d+|-\d+)?)(?P<Part>b)?(\s|_|\[|\()')),
    ('Ch_bare2', re.compile(r'^(?!Vol)(?P<Series>.*)\s?(?<!vol\. )\sChapter\s(?P<Chapter>\d+(?:\.?[\d-]+)?)')),
    ('Series_Dash_Ch', re.compile(r'^(?P<Series>.+?)\s-\s(?:Ch|Chapter)\.?\s(?P<Chapter>\d+(?:\.\d+)?)\.?(?:cbz|cbr)?$')),
    ('Series_Ch_Dash_Extra', re.compile(r'^(?P<Series>.+?)\s+Ch\.\s+(?P<Chapter>\d+(?:\.\d+)?)\s+-\s+(?P<Extra>.+)')),
    ('Series_Dot_Ch', re.compile(r'^(?P<Series>.+?)(?=\.\s*(?:Ch\.?|Chapter))\.\s*(?:Ch\.?|Chapter)\s*(?P<Chapter>\d+)(?:\s*-\s*(?P<Extra>.*?))?$')),
    ('Series_Dot_Vol_Ch_Extra', re.compile(r'^(?P<Series>.+?)\.?\s+Vol\.\s*(?P<Volume>\d+)\s+Ch\.\s*(?P<Chapter>\d+(?:\.\d+)?)\s+-\s+(?P<Extra>.+)')),
    ('Series_Ch_Number', re.compile(r'^(?P<Series>.+?)(?<!\s-)\s(?:Ch\.?|Chapter)\s?(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Series_Chapter', re.compile(r'^(?P<Series>.+?)(?<!\s[Vv]ol)(?<!\sVolume)\sChapter\s(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Series_Vol_Ch', re.compile(r'^(?P<Series>.+?)\sv(?P<Volume>\d+)(?:\s-\s)(?:Ch\.?|Chapter)\s?(?P<Chapter>\d+(?:\.\d+)?)')),
    ('Series_Dash_c_Year', re.compile(r'^(?P<Series>.+?)\s-\sc(?P<Chapter>\d+(?:\.\d+)?)\s+\((?P<Year>\d{4})\)(?:\s+\((?P<Extra>[^)]+)\))*')),
    ('Volume', re.compile(r'(?P<Title>.+?)\s(?:v|V)(?P<Volume>\d+)(?:\s-\s(?P<Extra>.*?))?\s*(?:\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Series_Bare_Chapter', re.compile(r'^(?P<Series>.+?)\s+(?P<Chapter>\d+(?:\.\d+)?)(?=$|\s|-)')),
    ('ChapterExtras', re.compile(r'(?P<Title>.+?)(?=\s+(?:c|ch|chapter)\b|\s+c\d)(?:.*?(?:c|ch|chapter))?\s*(?P<Chapter>\d+(?:\.\d+)?)?(?:\s-\s(?P<Extra>.*?))?(?:\s*\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Chapter', re.compile(r'(?P<Title>.+)\s(?:(?:c|ch|chapter)?\s*(?P<Chapter>\d+(?:\.\d+)?))?(?:\s-\s(?P<Extra>.*?))?\s*(?:\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))?')),
    ('Simple_Ch', re.compile(r'Chapter(?P<Chapter>\d+(-\d+)?)')),
    ('Vol_Chp', re.compile(r'(?P<Series>.*)(\s|_)(vol\d+)?(\s|_)Chp\.? ?(?P<Chapter>\d+)')),
    ('V_Ch', re.compile(r'v\d+\.(\s|_)(?P<Chapter>\d+(?:.\d+f|-\d+)?)')),
    ('Titled_Vol', re.compile(r'(?P<Series>.*?)\s-\sVol\.\s(?P<Volume>\d+)')),
    ('Bare_Ch', re.compile(r'^((?!v|vo|vol|Volume).)*(\s|_)(?P<Chapter>\.?\d+(?:.\d+|-\d+)?)(?P<Part>b)?(\s|_|\[|\()')),
    ('Vol_Chapter', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(Chp|Chapter)\.?(\s|_)?(?P<Chapter>\d+)')),
    ('Vol_Chapter2', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+)')),
    ('Vol_Chapter3', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)')),
    ('Vol_Chapter4', re.compile(r'(?P<Volume>((vol|volume|v))?(\s|_)?\.?\d+)(\s|_)(?P<Chapter>\d+(?:.\d+|-\d+)?)(\s|_)(?P<Extra>.*?)')),
    ('Series_Dash_Ch_Episode', re.compile(r'^(?P<Series>.+?)\s+-\s+c(?P<Chapter>\d+(?:\.\d+)?)\s+-\s+Episode\s+\d+(?:\s+\([^)]+\))*')),
#    ('Monolith', re.compile(r'(?P<Title>.+?)\s(?:(?:c|ch|chapter)?\s*(?P<Chapter>\d+(?:\.\d+)?))(?:\s-\s(?P<Extra>.*?))?(?:\s*\((?P<Year>\d{4})\))?\s*(?:\(Digital\))?\s*(?:\((?P<Source>[^)]+)\))')),
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
MAX_REALISTIC_CHAPTER = 500  # Maximum realistic chapter number

def setup_logging():
    # Create logger with daily rotation and proper formatting
    logger = logging.getLogger('MangaUnpacker')
    logger.setLevel(logging.INFO)

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
    file_handler.setLevel(logging.INFO)

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
    pattern_name = match_dict.get('_pattern_name')
    logger.debug(f"---\nScoring pattern '{pattern_name}' for filename '{filename}'")

    # Reject invalid
    if not validate_match(match_dict):
        logger.debug("  → validate_match failed, score=0")
        return 0

    # Clean filename
    clean_filename = os.path.splitext(filename)[0]
    clean_filename = re.sub(r'\s+\(\d{4}\).*$', '', clean_filename)

    # Track significant penalties for later use
    has_significant_penalty = False

    # Pattern‐specific bonuses
    if pattern_name and pattern_name.startswith('Webtoon_'):
        score += 5
        logger.debug(f"  +5 webtoon bonus → {score}")
    
    # Add specific bonus for One-Shot patterns
    elif pattern_name == 'Series_Oneshot_Year':
        score += 6  # Higher bonus to prioritize this specialized pattern
        logger.debug(f"  +6 One-Shot bonus → {score}")

    elif pattern_name == 'Series_Dash_Ch':
        score += 4
        logger.debug(f"  +4 Series_Dash_Ch bonus → {score}")

    elif pattern_name in ('Complete_Series', 'Complex_Series'):
        if match_dict.get('Series') and match_dict.get('Chapter') and match_dict.get('Year'):
            score += 3
            logger.debug(f"  +3 Complete/Complex bonus → {score}")

    # Group‐weight scoring
    for key, value in match_dict.items():
        if not value or not isinstance(value, str) or not value.strip():
            continue

        w = GROUP_WEIGHTS.get(key, 1)
        score += w
        logger.debug(f"  +{w} weight for '{key}'='{value}' → {score}")

        # Field-specific validation checks
        if key in ('Series', 'Title'):
            score_adjustment, penalty_flag = validate_series_field(value, clean_filename, score, pattern_name)
            score += score_adjustment
            has_significant_penalty = has_significant_penalty or penalty_flag
        elif key == 'Chapter':
            score_adjustment, penalty_flag = validate_chapter_field(value, score)
            score += score_adjustment
            has_significant_penalty = has_significant_penalty or penalty_flag
        elif key == 'Volume':
            score_adjustment, penalty_flag = validate_volume_field(value, score)
            score += score_adjustment
            has_significant_penalty = has_significant_penalty or penalty_flag
        elif key == 'Year':
            score_adjustment, penalty_flag = validate_year_field(value, score)
            score += score_adjustment
            has_significant_penalty = has_significant_penalty or penalty_flag

    # Essential components
    has_series = bool(match_dict.get('Series') or match_dict.get('Title'))
    has_number = bool(match_dict.get('Chapter') or match_dict.get('Volume'))
    
    # Check for field pollution (data in wrong fields)
    score_adjustment, pollution_penalty = check_field_pollution(match_dict)
    score += score_adjustment
    has_significant_penalty = has_significant_penalty or pollution_penalty
    logger.debug(f"  {score_adjustment:+d} field pollution check → {score}")
    
    # Only give essential components bonus if we have both AND no significant penalties
    if has_series and has_number and not has_significant_penalty:
        score += 5
        logger.debug(f"  +5 essential components bonus → {score}")
    elif not has_series:
        score -= 20
        logger.debug(f"  -20 missing series penalty → {score}")

    # Coverage bonus
    parts = ''.join(v for v in match_dict.values() if isinstance(v, str))
    cov = len(parts) / len(filename)
    if cov > 0.6 and not has_significant_penalty:
        score += 3
        logger.debug(f"  +3 coverage bonus ({cov:.2f}) → {score}")

    logger.debug(f"Final score for '{pattern_name}': {score}\n")
    return score

def validate_series_field(value, clean_filename, score, pattern_name):
    """Validates series/title field and returns score adjustment"""
    score_adjustment = 0
    has_significant_penalty = False
    sv = value.strip()
    
    # length bonus/penalty
    if len(sv) >= SERIES_MIN_LENGTH:
        bonus = min(len(sv)/10, 3)
        score_adjustment += bonus
        logger.debug(f"    +{bonus:.1f} length bonus → {score + score_adjustment}")
    else:
        score_adjustment -= 5
        logger.debug(f"    -5 too short penalty → {score + score_adjustment}")
    
    # pure‐digit
    if sv.isdigit():
        score_adjustment -= 8
        logger.debug(f"    -8 digit series penalty → {score + score_adjustment}")
    
    # endswith Chapter variations
    if re.search(r'(?i)\b(ch\.?|chapter)(\s+\d+)?$', sv):
        score_adjustment -= 10
        has_significant_penalty = True
        logger.debug(f"    -10 endswith Chapter penalty → {score + score_adjustment}")
    
    # trailing punctuation
    if sv.endswith(("-", ":", ".")) and not is_likely_abbreviation(sv):
        score_adjustment -= 8
        logger.debug(f"    -8 trailing punctuation penalty → {score + score_adjustment}")
    
    # volume indicator
    if re.search(r'(?i)\bvol(?:ume)?\.?\s*\d+\b|\sv\d+\b', sv):
        score_adjustment -= 15
        has_significant_penalty = True
        logger.debug(f"    -15 volume‐in‐series penalty → {score + score_adjustment}")
    
    # season indicator
    if not pattern_name or not pattern_name.startswith('Webtoon_'):
        if re.search(r'\bSeason\s*\d+\b|\(S\d+\)', sv):
            score_adjustment -= 15
            has_significant_penalty = True
            logger.debug(f"    -15 season‐in‐series penalty → {score + score_adjustment}")
    
    # too short relative
    if len(sv) < len(clean_filename)*0.3:
        # Don't penalize short series names for patterns that specifically handle this format
        if pattern_name in ('Complex_Series', 'Complex_Series2', 'Complex_SeriesDecimal'):
            # Check if filename has the pattern of series, 3-digit chapter, optional dash text, and year
            # Modified to allow additional content after the year
            if re.match(rf"^{re.escape(sv)}\s+\d{{3}}", clean_filename):
                logger.debug(f"    Skipping relative short series penalty for complex series format")
            else:
                score_adjustment -= 8
                logger.debug(f"    -8 relative short series penalty → {score + score_adjustment}")
    
    # single‐word vs multi‐word
    fnw = len(re.findall(r'\b\w+\b', clean_filename))
    sgw = len(re.findall(r'\b\w+\b', sv))
    if sgw==1 and fnw>=3:
        # Don't penalize single-word series for complex patterns that have this format
        if pattern_name in ('Complex_Series', 'Complex_Series2', 'Complex_SeriesDecimal'):
            # Check if this looks like a manga with 3-digit chapter
            if re.match(rf"^{re.escape(sv)}\s+\d{{3}}", clean_filename):
                logger.debug(f"    Skipping single-word series penalty for complex series format")
            else:
                score_adjustment -= 10
                logger.debug(f"    -10 single‐word series penalty → {score + score_adjustment}")
        else:
            score_adjustment -= 10
            logger.debug(f"    -10 single‐word series penalty → {score + score_adjustment}")
    
    # word overlap
    if fnw>=3:
        fset = set(w.lower() for w in re.findall(r'\b\w+\b', clean_filename))
        sset = set(w.lower() for w in re.findall(r'\b\w+\b', sv))
        if len(fset & sset) < len(fset)*0.3:
            # Don't penalize low overlap for complex series patterns
            if pattern_name in ('Complex_Series', 'Complex_Series2', 'Complex_SeriesDecimal'):
                if re.match(rf"^{re.escape(sv)}\s+\d{{3}}", clean_filename):
                    logger.debug(f"    Skipping low overlap penalty for complex series format")
                else:
                    score_adjustment -= 8
                    logger.debug(f"    -8 low overlap penalty → {score + score_adjustment}")
            else:
                score_adjustment -= 8
                logger.debug(f" {fset} & {sset} → low overlap penalty")
                logger.debug(f"    -8 low overlap penalty → {score + score_adjustment}")
    
    # Chapter number in series name (not at end)
    if re.search(r'(?i)\b(?:ch|chapter)\s*\d+\b(?!$)', sv):
        score_adjustment -= 12
        logger.debug(f"    -12 chapter-in-series penalty → {score + score_adjustment}")
    
    # Episode identifier in series name
    if re.search(r'(?i)\bepisode\s*\d+\b', sv) and not pattern_name.startswith('Webtoon_'):
        score_adjustment -= 10
        logger.debug(f"    -10 episode-in-series penalty → {score + score_adjustment}")
    
    # Check for repeated numbers pattern
    if re.search(r'\b(\d+)\b.*\b\1\b', clean_filename):
        # If series ends with a number that repeats later in the filename
        number_match = re.search(r'\b(\d+)$', sv)
        if number_match:
            number = number_match.group(1)
            # Now use the actual number in the pattern instead of \1 backreference
            if re.search(rf"{re.escape(sv)}\s+-\s+\w+\s+{number}", clean_filename):
                score_adjustment -= 15
                has_significant_penalty = True
                logger.debug(f"    -15 series ends with number that repeats in filename → {score + score_adjustment}")
    
    return score_adjustment, has_significant_penalty

def validate_chapter_field(value, score):
    """Validates chapter field and returns score adjustment"""
    score_adjustment = 0
    has_significant_penalty = False
    
    try:
        ch = float(value)
        if 0 <= ch <= MAX_REALISTIC_CHAPTER:  # allow chapter zero
            score_adjustment += 2
            logger.debug(f"    +2 realistic chapter bonus → {score + score_adjustment}")
        else:
            score_adjustment -= 5
            logger.debug(f"    -5 unrealistic chapter penalty → {score + score_adjustment}")
    except ValueError:
        if '-' in value:
            score_adjustment += 1
            logger.debug(f"    +1 range chapter bonus → {score + score_adjustment}")
        else:
            score_adjustment -= 2
            logger.debug(f"    -2 non‐numeric chapter penalty → {score + score_adjustment}")
    
    # Check for text that belongs in series
    if re.search(r'[a-zA-Z]{4,}', value) and not re.search(r'(?i)part|side|extra', value):
        score_adjustment -= 10
        has_significant_penalty = True
        logger.debug(f"    -10 text-in-chapter penalty → {score + score_adjustment}")
    
    return score_adjustment, has_significant_penalty

def validate_volume_field(value, score):
    """Validates volume field and returns score adjustment"""
    score_adjustment = 0
    has_significant_penalty = False
    
    MAX_REALISTIC_VOLUME = 100  # Most series don't go beyond this
    
    try:
        vol = float(value)
        if 1 <= vol <= MAX_REALISTIC_VOLUME:
            score_adjustment += 1
            logger.debug(f"    +1 realistic volume bonus → {score + score_adjustment}")
        else:
            score_adjustment -= 3
            logger.debug(f"    -3 unrealistic volume penalty → {score + score_adjustment}")
    except ValueError:
        score_adjustment -= 3
        logger.debug(f"    -3 non-numeric volume penalty → {score + score_adjustment}")
    
    # Check for text that belongs in series
    if re.search(r'[a-zA-Z]{4,}', value):
        score_adjustment -= 8
        has_significant_penalty = True
        logger.debug(f"    -8 text-in-volume penalty → {score + score_adjustment}")
    
    return score_adjustment, has_significant_penalty

def validate_year_field(value, score):
    """Validates year field and returns score adjustment"""
    score_adjustment = 0
    has_significant_penalty = False
    
    current_year = date.today().year
    
    try:
        year = int(value)
        if 1900 <= year <= current_year + 5:  # Allow some future dates
            score_adjustment += 1
            logger.debug(f"    +1 realistic year bonus → {score + score_adjustment}")
        else:
            score_adjustment -= 4
            has_significant_penalty = True
            logger.debug(f"    -4 unrealistic year penalty → {score + score_adjustment}")
    except ValueError:
        score_adjustment -= 5
        has_significant_penalty = True
        logger.debug(f"    -5 invalid year penalty → {score + score_adjustment}")
    
    return score_adjustment, has_significant_penalty

def check_field_pollution(match_dict):
    """Check for data that appears in the wrong fields"""
    score_adjustment = 0
    has_significant_penalty = False
    
    # Check for Series data in Chapter field
    chapter = match_dict.get('Chapter', '')
    if isinstance(chapter, str) and re.search(r'[a-zA-Z]{5,}', chapter):
        if not re.search(r'(?i)part|side|extra', chapter):
            score_adjustment -= 15
            has_significant_penalty = True
            logger.debug(f"  -15 series text in chapter field")
    
    # Check for Chapter data in Series field
    series = match_dict.get('Series', '') or match_dict.get('Title', '')
    if isinstance(series, str):
        # If series ends with something like "Chapter 123"
        if re.search(r'(?i)chapter\s+\d+$', series.strip()):
            score_adjustment -= 15
            has_significant_penalty = True
            logger.debug(f"  -15 chapter data at end of series field")
        elif re.search(r'(?i)\bch\.?\s*\d+', series.strip()):
            score_adjustment -= 15
            has_significant_penalty = True
            logger.debug(f"  -15 Ch. indicator in series field")
        elif re.search(r'\bc\d+\b', series.strip()):
            score_adjustment -= 15
            has_significant_penalty = True
            logger.debug(f"  -15 c### chapter indicator in series field")
    
    # Check for Volume data in Series field
    if isinstance(series, str):
            # If series has explicit volume indicator
            vol_match = re.search(r'(?i)\bvol(?:ume)?\s*\d+\b|\sv\d+\b', series)
            if vol_match:
                # Stricter check: if the volume indicator is the entire series name
                if vol_match.group() == series.strip():
                    score_adjustment -= 15
                    has_significant_penalty = True
                    logger.debug(f"  -15 volume-only series field")
                # Add separate penalty for volume at end of series name
                elif re.search(r'\sv\d+$', series.strip()):
                    score_adjustment -= 10
                    has_significant_penalty = True
                    logger.debug(f"  -10 volume identifier at end of series name")
    
    # Check for Volume data in Chapter field
    if isinstance(chapter, str):
        # Check for both 'vol##' and 'v##' formats in chapter field
        if re.search(r'(?i)\bvol(?:ume)?\s*\d+\b|\bv\d+\b', chapter):
            if not re.search(r'\d+\s*-\s*\d+', chapter):  # Allow ranges
                score_adjustment -= 10
                logger.debug(f"  -10 volume data in chapter field")

    # Detect when the same number appears at the end of series and as chapter number
    chapter = match_dict.get('Chapter', '')
    series = match_dict.get('Series', '') or match_dict.get('Title', '')
    if chapter and series and chapter.isdigit():
        # Check if series ends with same number as chapter
        if series.strip().endswith(f" {chapter}"):
            score_adjustment -= 20
            has_significant_penalty = True
            logger.debug(f"  -20 series ends with same number as chapter")
        # Check for the chapter number followed by space+hyphen+space (common in titles with repetition)
        elif re.search(rf"\s{chapter}\s+-\s+", series):
            score_adjustment -= 25
            has_significant_penalty = True
            logger.debug(f"  -25 series contains chapter followed by hyphen pattern")
    
    return score_adjustment, has_significant_penalty

def validate_match(match_dict):
    """Additional validation checks for a matched pattern"""
    for key, value in match_dict.items():
        if isinstance(value, str):
            if key in ('Series', 'Title'):
                # Check if trailing dash is likely part of a "word-word-" pattern
                if value.strip().endswith('-'):
                    # For series names, just check if there's enough content before the dash
                    # This is more permissive and handles a wider range of title formats
                    if len(value.strip()) > 5:  # If there's a reasonable amount of content
                        continue  # Accept trailing dash for series names with decent length
                
                # Only fail for trailing periods that aren't part of abbreviations
                if value.strip().endswith(('-', ':', '.')) and not is_likely_abbreviation(value.strip()):
                    logger.debug(f"  validate_match failed: {key} '{value}' ends with invalid character")
                    return False
                
                if value.strip().endswith(('Ch.', 'Ch', 'Chapter')):
                    logger.debug(f"  validate_match failed: {key} '{value}' ends with chapter indicator")
                    return False
                    
                if re.fullmatch(r'(?i)(?:vol(?:ume)?|v)\.?\s*\d+', value.strip()):
                    logger.debug(f"  validate_match failed: {key} '{value}' is just a volume indicator")
                    return False

    return True

def is_likely_abbreviation(text_segment):
    """Detect if a period is part of a common abbreviation pattern"""
    # Common business/title abbreviations
    common_abbr = ['co.', 'ltd.', 'inc.', 'llc.', 'corp.', 'dr.', 'mr.', 'mrs.', 'ms.', 'jr.', 'sr.', 'vs.', 'etc.']
    
    # Check for common abbreviations
    for abbr in common_abbr:
        if text_segment.lower().endswith(abbr):
            return True
    
    # Check for single letter followed by period (like A., B., etc.)
    if re.search(r'\b[A-Z]\.\s*', text_segment):
        return True
            
    return False

def match_best_pattern(filename, auto_mode=False):
    """Find the best pattern match for a filename.
    
    Args:
        filename: The filename to match
        auto_mode: Whether to run in automatic mode without prompts
        
    Returns:
        Tuple of (pattern_name, match_dict) or None if no match
    """
    global _stored_pattern_choice, _stored_manual_pattern, _stored_skip_series
    
    force_interactive = False

    # 1. Check if we should skip this file based on series name
    if _stored_skip_series and filename.startswith(_stored_skip_series['series_prefix']):
        logger.debug(f"Automatically skipping file from series: {_stored_skip_series['series']}")
        print(f"{Fore.YELLOW}⚠ Automatically skipping: {filename}")
        return None

    # Extract potential series name for comparison and clearing checks
    potential_series = extract_series_name(filename)
    
    # Clear session patterns if they're for this series to prevent re-using a bad pattern
    if _stored_pattern_choice and potential_series and _stored_pattern_choice.get('series_prefix') == potential_series:
        logger.debug(f"Clearing stored pattern for '{potential_series}' before matching to prevent re-using problematic pattern")
        _stored_pattern_choice = None
    
    # 2. Try to get a match from the database  
    db_match_result = try_database_match(filename, auto_mode)
    if db_match_result:
        # Check for the special marker to force interactive selection
        if db_match_result[0] == "FORCE_INTERACTIVE":
            force_interactive = True
            logger.info(f"Forcing interactive selection due to series name mismatch")
        else:
            return db_match_result

    # 3. Check for stored pattern choice from current session
    if not force_interactive:  # Only try session patterns if not forcing interactive
        session_match = try_session_stored_patterns(filename)
        if session_match:
            return session_match
    
    # 4. Try all patterns and collect matches with scores
    all_matches = score_all_patterns(filename)
    
    # Log top matches for debugging
    log_top_matches(filename, all_matches)

    # 5. Handle selection based on mode
    if auto_mode and not force_interactive:  # Only use auto mode if not forcing interactive
        return handle_auto_mode_selection(all_matches, filename)
    else:
        # Pass the force_interactive flag to handle_interactive_selection
        return handle_interactive_selection(filename, all_matches, force_interactive)

def try_database_match(filename, auto_mode):
    """Try to match using stored database patterns."""
    global _stored_pattern_choice
    
    db_match = get_stored_pattern(filename)
    if not db_match:
        return None
        
    pattern_name = db_match['pattern_name']
    series_name = db_match['series_name']
    logger.debug(f"Found database pattern '{pattern_name}' for {filename}")
    
    # Check for format change in the file structure
    format_changed, change_info = detect_format_change(series_name, filename)
    if format_changed:
        logger.info(f"Format change detected for '{series_name}' - prompting for new pattern selection")
        print(f"{Fore.YELLOW}⚠ File format change detected for '{series_name}'")
        
        print(f"{Fore.YELLOW}  Stored pattern: {Fore.CYAN}{pattern_name}")
        
        if change_info['change_type']:
            print(f"{Fore.YELLOW}  Format changes detected:")
            for change in change_info['change_type']:
                print(f"{Fore.YELLOW}    • {change}")
                
        print(f"{Fore.YELLOW}  Previous example: {Fore.CYAN}{os.path.basename(change_info['previous']['example'])}")
        print(f"{Fore.YELLOW}  Current file: {Fore.CYAN}{os.path.basename(filename)}")
        
        if auto_mode:
            # In auto mode, proceed with pattern matching instead of prompting
            logger.info("Auto mode - proceeding with pattern matching")
            print(f"{Fore.YELLOW}  Auto mode - trying to find best pattern...")
            return None
        else:
            confirm = input(f"{Fore.GREEN}➤ Use stored pattern anyway? (y/n): {Fore.WHITE}").strip().lower()
            if confirm != 'y':
                print(f"{Fore.GREEN}✓ Will select a new pattern for this file format.")
                return None
            print(f"{Fore.GREEN}✓ Using stored pattern despite format change.")
    
    # Process manual patterns
    if db_match['is_manual']:
        return handle_manual_pattern(db_match, filename, pattern_name)
    
    # Process regular (non-manual) patterns by re-applying regex
    result = handle_regular_pattern(db_match, filename)
    
    if result is None:
        # If database pattern was rejected, also clear session stored patterns
        # to prevent using the same problematic pattern from the session
        _stored_pattern_choice = None
        logger.debug(f"Cleared session stored pattern after database pattern rejection")
        
        # Force interactive selection for this file by returning a special marker
        if not auto_mode:  # Only force interactive mode when not in auto mode
            return "FORCE_INTERACTIVE", None
    
    return result


def handle_manual_pattern(db_match, filename, pattern_name):
    """Handle a manual pattern match from the database."""
    series_name = db_match['series_name']
    
    # Create a manual match dict
    match_dict = {'Series': series_name, '_pattern_name': pattern_name}
    
    # Add chapter if we can extract one
    if filename.startswith(series_name):
        remainder = filename[len(series_name):].strip()
        # Try to extract chapter number
        chapter_match = re.search(r'^\s*(\d+(?:\.\d+)?)', remainder)
        if not chapter_match:
            chapter_match = re.search(r'\b(\d+(?:\.\d+)?)\b', remainder)
        if chapter_match:
            match_dict['Chapter'] = chapter_match.group(1)
        
        # Add volume detection
        volume_match = re.search(r'\bv(?:ol)?\.?\s*(\d+)\b', remainder, re.IGNORECASE)
        if volume_match:
            match_dict['Volume'] = volume_match.group(1)
    
    # Return the manual match
    return pattern_name, match_dict


def handle_regular_pattern(db_match, filename):
    """Handle a regular (non-manual) pattern from the database."""
    pattern_name = db_match['pattern_name']
    series_name = db_match['series_name']
    
    logger.debug(f"Attempting to re-apply DB pattern '{pattern_name}' for series '{series_name}' to file '{filename}'")

    # Find the regex pattern to apply
    regex_to_apply = None
    for name_iter, pattern_regex_iter in patterns:
        if name_iter == pattern_name:
            regex_to_apply = pattern_regex_iter
            break
    
    if not regex_to_apply:
        logger.warning(f"Pattern name '{pattern_name}' from database (for series '{series_name}') not found in current `patterns` list")
        return None
    
    # Try to apply the pattern
    match_obj = regex_to_apply.match(filename)
    if not match_obj:
        logger.info(f"Stored DB pattern '{pattern_name}' (for series '{series_name}') did not re-match the current file '{filename}'")
        return None
    
    # Successfully re-applied the regex
    newly_parsed_dict = match_obj.groupdict()
    newly_parsed_dict['_pattern_name'] = pattern_name
    
    # Check if the series parsed from the current file matches the one in DB
    current_series_parse = newly_parsed_dict.get('Series') or newly_parsed_dict.get('Title')
    if current_series_parse and current_series_parse.strip() != series_name.strip():
        logger.warning(f"Pattern mismatch detected: '{pattern_name}' parsed this file as '{current_series_parse}' but expected '{series_name}'")
        print(f"\n{Fore.YELLOW}⚠ Series name mismatch detected!")
        print(f"{Fore.WHITE}  Expected: {Fore.CYAN}{series_name}")
        print(f"{Fore.WHITE}  Parsed as: {Fore.CYAN}{current_series_parse}")
        print(f"{Fore.WHITE}  Showing all available matching options instead.")
        
        # Force selection of a new pattern by returning None
        return None
    
    logger.debug(f"DB pattern '{pattern_name}' re-applied successfully. New match_dict: {newly_parsed_dict}")
    return pattern_name, newly_parsed_dict


def try_session_stored_patterns(filename):
    """Try to match using patterns stored in the current session."""
    global _stored_pattern_choice, _stored_manual_pattern
    
    # Check if we have a stored pattern choice that applies
    if _stored_pattern_choice and filename.startswith(_stored_pattern_choice['series_prefix']):
        stored = _stored_pattern_choice
        logger.debug(f"Using stored pattern '{stored['pattern_name']}' for '{filename}'")
        
        # Find and apply the pattern
        for pattern_name, pattern in patterns:
            if pattern_name == stored['pattern_name']:
                match = pattern.match(filename)
                if match and match.groupdict():
                    match_dict = match.groupdict()
                    match_dict['_pattern_name'] = pattern_name
                    
                    # Add series name validation just like in handle_regular_pattern
                    current_series_parse = match_dict.get('Series') or match_dict.get('Title')
                    expected_series = stored['match_dict'].get('Series') or stored['match_dict'].get('Title')
                    
                    if current_series_parse and expected_series and current_series_parse.strip() != expected_series.strip():
                        # Same validation as in handle_regular_pattern
                        logger.warning(f"Session pattern mismatch: '{pattern_name}' parsed this file as '{current_series_parse}' but expected '{expected_series}'")
                        print(f"\n{Fore.YELLOW}⚠ Series name mismatch in session pattern!")
                        print(f"{Fore.WHITE}  Expected: {Fore.CYAN}{expected_series}")
                        print(f"{Fore.WHITE}  Parsed as: {Fore.CYAN}{current_series_parse}")
                        print(f"{Fore.WHITE}  Showing all available matching options instead.")
                        
                        # Clear the stored pattern to prevent it from being reused
                        _stored_pattern_choice = None
                        return None
                        
                    return pattern_name, match_dict
    
    # Check if we have a stored manual pattern that applies
    if _stored_manual_pattern and filename.startswith(_stored_manual_pattern['series_prefix']):
        stored = _stored_manual_pattern
        # For manual patterns, update the chapter number
        match_dict = stored['match_dict'].copy()
        
        # Extract chapter number from filename
        chapter_match = re.search(r'\b(\d+(?:\.\d+)?)\b', filename[len(stored['series_prefix']):])
        if chapter_match:
            match_dict['Chapter'] = chapter_match.group(1)
            logger.debug(f"Using stored manual pattern for '{filename}' with chapter {match_dict['Chapter']}")
            return "Manual Entry", match_dict
    
    return None


def score_all_patterns(filename):
    """Try all patterns and return scored matches."""
    all_matches = []
    
    for pattern_name, pattern in patterns:
        match = pattern.match(filename)
        if match:
            match_dict = match.groupdict()
            match_dict['_pattern_name'] = pattern_name
            match_dict['_original_filename'] = filename
            score = score_match(match_dict, filename)
            all_matches.append((pattern_name, match_dict, score))
    
    # Sort matches by score in descending order
    all_matches.sort(key=lambda x: x[2], reverse=True)
    return all_matches


def log_top_matches(filename, all_matches):
    """Log the top matches for debugging."""
    logger.debug(f"Top matches for '{filename}':")
    for i, (pattern_name, match_dict, score) in enumerate(all_matches[:3]):
        if i == 0 or score > 0:  # Only show positive scores beyond the top match
            logger.debug(f"  {pattern_name}: {match_dict} (Score: {score})")


def handle_auto_mode_selection(all_matches, filename):
    """Handle automatic selection of the best match."""
    if all_matches and all_matches[0][2] > 0:
        logger.info(f"Auto-selecting best match for '{filename}': {all_matches[0][0]} (Score: {all_matches[0][2]})")
        return all_matches[0][0], all_matches[0][1]
    else:
        logger.warning(f"No good matches found for '{filename}' in auto mode - skipping")
        return None


def handle_interactive_selection(filename, all_matches, force_interactive=False):
    """Handle interactive selection with user input."""
    # ADDED: If we're forcing interactive selection due to mismatch, always show options
    if force_interactive and all_matches and len(all_matches) > 0:
        print(f"{Fore.YELLOW}Series name mismatch requires manual pattern selection")
        return handle_multiple_matches(filename, all_matches)
    
    # Standard logic for normal cases follows
    # Close scores, ask user
    if len(all_matches) > 1 and all_matches[0][2] > 0 and all_matches[0][2] - all_matches[1][2] < 4:
        return handle_multiple_matches(filename, all_matches)
    
    # Low confidence in best match
    if all_matches and all_matches[0][2] < 5:
        return handle_low_confidence_match(filename, all_matches)
    
    # Return best match if it has a positive score
    if all_matches and all_matches[0][2] > 0:
        return all_matches[0][0], all_matches[0][1]
    
    # No good matches found
    return handle_no_matches(filename)

def get_destination_preview(match_dict):
    """Preview how a file would be named and organized in the library."""
    series = match_dict.get('Series') or match_dict.get('Title', 'Unknown')
    volume = match_dict.get('Volume')
    chapter = match_dict.get('Chapter')
    
    # Special handling for one-shots based on pattern name
    is_oneshot = match_dict.get('_pattern_name') == 'Series_Oneshot_Year'
    
    file_name = series
    if volume:
        file_name += f" v{volume}"
    if is_oneshot:
        file_name += " - One-shot"
    elif chapter:
        file_name += f" - Chapter {chapter}"
    file_name += ".cbz"

    return f"📂 {Style.BRIGHT + Fore.CYAN}{series}/{Style.BRIGHT + Fore.GREEN}{file_name}{Fore.WHITE}"

def group_functionally_identical_matches(all_matches):
    """Group matches that produce the same functional result."""
    grouped_matches = {}
    
    for pattern_name, match_dict, score in all_matches:
        if score <= 0:
            continue
            
        # Create a key based on the essential extraction fields
        series = match_dict.get('Series') or match_dict.get('Title', 'Unknown')
        chapter = match_dict.get('Chapter', 'N/A') 
        volume = match_dict.get('Volume', 'N/A')
        
        key = f"{series}|{chapter}|{volume}"
        
        if key not in grouped_matches:
            grouped_matches[key] = {
                'patterns': [],
                'match_dict': match_dict,
                'best_score': score,
                'series': series,
                'chapter': chapter,
                'volume': volume
            }
        
        grouped_matches[key]['patterns'].append((pattern_name, score))
        
        # Update if this pattern has a higher score
        if score > grouped_matches[key]['best_score']:
            grouped_matches[key]['best_score'] = score
            grouped_matches[key]['match_dict'] = match_dict
    
    # Convert to list sorted by best score in each group
    result = list(grouped_matches.values())
    result.sort(key=lambda x: x['best_score'], reverse=True)
    
    return result

def handle_multiple_matches(filename, all_matches):
    """Handle the case where multiple patterns have similar scores."""
    # Get matches with positive scores
    positive_matches = [(p, m, s) for p, m, s in all_matches if s > 0]
    if not positive_matches:
        return handle_no_matches(filename)
        
    # Group functionally identical matches
    grouped_matches = group_functionally_identical_matches(positive_matches)
    
    # Show the options to the user
    print(f"\n{Fore.GREEN}Multiple matches found for: {Fore.CYAN}{filename}")
    
    for i, group in enumerate(grouped_matches):
        match_dict = group['match_dict']
        best_score = group['best_score']
        patterns = group['patterns']
        
        # Get preview of destination
        dest_preview = get_destination_preview(match_dict)
        
        # Create a clear, colorful display of each option
        print(f"\n{Fore.WHITE}{i+1}. {Fore.YELLOW}Option {i+1} {Fore.WHITE}(Score: {Fore.GREEN}{best_score:.1f}{Fore.WHITE})")
        
        # Show series, chapter, volume - only if they exist
        print(f"   {Fore.WHITE}Series: {Fore.CYAN}{group['series']}")
        if group['chapter'] != 'N/A':
            print(f"   {Fore.WHITE}Chapter: {Fore.CYAN}{group['chapter']}")
        if group['volume'] != 'N/A':
            print(f"   {Fore.WHITE}Volume: {Fore.CYAN}{group['volume']}")
        
        # Show the file organization preview
        print(f"   {Fore.WHITE}Will organize as: {Fore.WHITE}{dest_preview}")

        # Show pattern information (condensed)
        if len(patterns) > 1:
            pattern_names = [p[0] for p in sorted(patterns, key=lambda x: x[1], reverse=True)[:3]]
            if len(patterns) > 3:
                pattern_str = f"{', '.join(pattern_names)} and {len(patterns)-3} more"
            else:
                pattern_str = ', '.join(pattern_names)
            print(f"   {Fore.WHITE}Matching patterns: {Fore.BLUE}{pattern_str}")
        else:
            # Always show pattern information even when there's only one pattern
            pattern_name = patterns[0][0]
            print(f"   {Fore.WHITE}Pattern: {Fore.BLUE}{pattern_name}")
    
    # Ask for user choice
    while True:
        choice = input(f"\n{Fore.GREEN}➤ Choose best match (1-{len(grouped_matches)}), 'm' for manual entry, "
                      f"'s' to skip this file, or 'a' to skip all files from this series: {Fore.WHITE}")
        
        if choice.lower() == 'm':
            return handle_no_matches(filename)
        
        elif choice.lower() == 's':
            print(f"{Fore.YELLOW}Skipping file: {filename}")
            logger.info(f"User chose to skip file: {filename}")
            return None
            
        elif choice.lower() == 'a':
            # Get series name to skip from best match or extract directly
            best_series = None
            if grouped_matches:
                best_series = grouped_matches[0]['series']
                
            if not best_series:
                best_series = extract_series_name(filename)
                
            print(f"{Fore.YELLOW}Will skip all files from series: {Fore.CYAN}{best_series}")
            
            # Store the skip preference for this session
            global _stored_skip_series
            _stored_skip_series = {
                'series': best_series,
                'series_prefix': best_series
            }
            
            logger.info(f"User chose to skip all files from series: {best_series}")
            return None
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(grouped_matches):
                group = grouped_matches[idx]
                match_dict = group['match_dict']
                
                # Find the highest scoring pattern in this group
                best_pattern = sorted(group['patterns'], key=lambda x: x[1], reverse=True)[0]
                best_pattern_name = best_pattern[0]
                
                # Remember this pattern for future files
                series = group['series']
                if series:
                    global _stored_pattern_choice
                    _stored_pattern_choice = {
                        'pattern_name': best_pattern_name,
                        'series_prefix': series,
                        'match_dict': match_dict
                    }
                    
                    # Store in database for future sessions
                    store_pattern_match(series, best_pattern_name)
                    logger.info(f"User selected pattern '{best_pattern_name}' for '{series}'")
                
                return best_pattern_name, match_dict
        except ValueError:
            print(f"{Fore.RED}Please enter a number between 1 and {len(grouped_matches)}, 'm', 's', or 'a'")

def handle_low_confidence_match(filename, all_matches):
    """Handle the case where the best match has a low confidence score."""
    best_match = all_matches[0]
    pattern_name, match_dict, score = best_match
    
    # Show match details
    series = match_dict.get('Series') or match_dict.get('Title', 'Unknown')
    chapter = match_dict.get('Chapter', 'N/A')
    volume = match_dict.get('Volume', 'N/A')
    
    print(f"\n{Fore.YELLOW}Low confidence match for: {Fore.CYAN}{filename}")
    print(f"{Fore.WHITE}Best pattern: {Fore.YELLOW}{pattern_name} {Fore.WHITE}(Score: {Fore.YELLOW}{score}{Fore.WHITE})")
    print(f"{Fore.WHITE}Series: {Fore.CYAN}{series}")
    print(f"{Fore.WHITE}Chapter: {Fore.CYAN}{chapter}")
    if volume != 'N/A':
        print(f"{Fore.WHITE}Volume: {Fore.CYAN}{volume}")
    
    # Ask for confirmation
    while True:
        choice = input(f"\n{Fore.GREEN}➤ Use this match? (y/n/m for manual): {Fore.WHITE}")
        
        if choice.lower() == 'y':
            # Remember this pattern for future files
            if series:
                global _stored_pattern_choice
                _stored_pattern_choice = {
                    'pattern_name': pattern_name,
                    'series_prefix': series,
                    'match_dict': match_dict
                }
                
                # Store in database for future sessions
                store_pattern_match(series, pattern_name)
                logger.info(f"User confirmed low-confidence pattern '{pattern_name}' for '{series}'")
            
            return pattern_name, match_dict
        
        elif choice.lower() == 'n':
            # Try another match if available
            if len(all_matches) > 1 and all_matches[1][2] > 0:
                return handle_multiple_matches(filename, all_matches)
            else:
                return handle_no_matches(filename)
        
        elif choice.lower() == 'm':
            return handle_no_matches(filename)


def handle_no_matches(filename):
    """Handle the case where no good matches are found."""
    print(f"\n{Fore.RED}No good pattern matches found for: {Fore.CYAN}{filename}")
    
    while True:
        print(f"\n{Fore.YELLOW}Options:")
        print(f"{Fore.WHITE}1. Enter series name manually")
        print(f"{Fore.WHITE}2. Skip this file")
        print(f"{Fore.WHITE}3. Skip all files from this series")
        
        choice = input(f"\n{Fore.GREEN}➤ Choose an option (1-3): {Fore.WHITE}")
        
        if choice == '1':
            # Manual series entry
            series_name = input(f"{Fore.GREEN}➤ Enter series name: {Fore.WHITE}").strip()
            
            if not series_name:
                print(f"{Fore.RED}Series name cannot be empty")
                continue
            
            # Try to extract chapter number
            chapter_match = re.search(r'\b(\d+(?:\.\d+)?)\b', filename[len(series_name):].strip())
            chapter = chapter_match.group(1) if chapter_match else None
            
            # Try to extract volume number
            volume_match = re.search(r'\bv(?:ol)?\.?\s*(\d+)\b', filename, re.IGNORECASE)
            volume = volume_match.group(1) if volume_match else None
            
            match_dict = {'Series': series_name}
            if chapter:
                match_dict['Chapter'] = chapter
            if volume:
                match_dict['Volume'] = volume
            
            # Remember this manual pattern for future files
            global _stored_manual_pattern
            _stored_manual_pattern = {
                'series_prefix': series_name,
                'match_dict': match_dict
            }
            
            # Store in database as a manual pattern
            store_pattern_match(series_name, "Manual Entry", is_manual=True, raw_match=match_dict)
            logger.info(f"User manually entered series '{series_name}' for '{filename}'")
            
            return "Manual Entry", match_dict
        
        elif choice == '2':
            # Skip this file only
            logger.info(f"User chose to skip file: {filename}")
            return None
        
        elif choice == '3':
            # Skip all files from this series
            # Extract potential series name using best-effort approach
            potential_series = extract_series_name(filename)
            
            # Confirm the series name to skip
            print(f"{Fore.YELLOW}Detected series: {Fore.CYAN}{potential_series}")
            confirm_series = input(f"{Fore.GREEN}➤ Is this the correct series name to skip? (y/n): {Fore.WHITE}")
            
            if confirm_series.lower() == 'y':
                # Store the skip preference for this session
                global _stored_skip_series
                _stored_skip_series = {
                    'series': potential_series,
                    'series_prefix': potential_series
                }
                
                logger.info(f"User chose to skip all files from series: {potential_series}")
                return None
            else:
                # Let them enter the series name manually
                skip_series = input(f"{Fore.GREEN}➤ Enter series name to skip: {Fore.WHITE}").strip()
                if skip_series:
                    _stored_skip_series = {
                        'series': skip_series,
                        'series_prefix': skip_series
                    }
                    
                    logger.info(f"User chose to skip all files from series: {skip_series}")
                    return None

_stored_format_types = {}  # {'series_name': {'has_volume': bool, 'has_chapter': bool}}

def detect_format_change(series_name, filename):
    """Detects if a file's format differs from previously processed files in the same series.
    Returns tuple (bool, dict) where bool indicates format change and dict contains details."""
    
    # Check if we have stored format information for this series
    if series_name not in _stored_format_types:
        # First file for this series, detect and store its format
        has_volume = bool(re.search(r'\bv(?:ol)?\.?\s*\d+\b', filename, re.IGNORECASE))
        has_chapter = bool(re.search(r'\b(?:ch\.?|chapter)\s*\d+(?:\.\d+)?\b|\s\d{2,3}(?:\.\d+)?\s', filename, re.IGNORECASE))
        
        _stored_format_types[series_name] = {
            'has_volume': has_volume,
            'has_chapter': has_chapter,
            'example_filename': filename
        }
        return False, {}  # No change, first file
    
    # Check current file format
    current_has_volume = bool(re.search(r'\bv(?:ol)?\.?\s*\d+\b', filename, re.IGNORECASE))
    current_has_chapter = bool(re.search(r'\b(?:ch\.?|chapter)\s*\d+(?:\.\d+)?\b|\s\d{2,3}(?:\.\d+)?\s', filename, re.IGNORECASE))
    
    # Detect format change
    prev_format = _stored_format_types[series_name]
    format_changed = (prev_format['has_volume'] != current_has_volume) or (prev_format['has_chapter'] != current_has_chapter)
    
    # Create info dictionary with detailed change information
    change_info = {}
    if format_changed:
        change_info = {
            'previous': {
                'has_volume': prev_format['has_volume'],
                'has_chapter': prev_format['has_chapter'],
                'example': prev_format.get('example_filename', 'unknown')
            },
            'current': {
                'has_volume': current_has_volume,
                'has_chapter': current_has_chapter,
                'example': filename
            },
            'change_type': []
        }
        
        # Specify the type of change
        if prev_format['has_volume'] != current_has_volume:
            if current_has_volume:
                change_info['change_type'].append("Added volume numbering")
            else:
                change_info['change_type'].append("Removed volume numbering")
                
        if prev_format['has_chapter'] != current_has_chapter:
            if current_has_chapter:
                change_info['change_type'].append("Added chapter numbering")
            else:
                change_info['change_type'].append("Removed chapter numbering")
        
        logger.debug(f"Format change detected for '{series_name}': " 
                    f"Previous: vol={prev_format['has_volume']}, ch={prev_format['has_chapter']} | "
                    f"Current: vol={current_has_volume}, ch={current_has_chapter}")
        
        # Update stored format for future comparisons
        _stored_format_types[series_name] = {
            'has_volume': current_has_volume,
            'has_chapter': current_has_chapter,
            'example_filename': filename
        }
    
    return format_changed, change_info

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
            
            print(f"{Fore.WHITE}      • Extracting {Fore.YELLOW}{file}")
            subprocess.call(['unrar', 'x', rar_path, folder_name])
            num_extracted = len([f for f in os.listdir(folder_name) 
                               if os.path.isfile(os.path.join(folder_name, f))])
            print(f"{Fore.WHITE}      • Extracted {Fore.GREEN}{num_extracted} files")
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
                logger.debug(f"Moved processed folder to !Finished: {folder_name}")
            except Exception as e:
                print(f"{Fore.RED}  ✘ Error moving folder {folder_name}: {str(e)}")
                logger.error(f"Error moving folder {folder_name} to !Finished: {str(e)}")
        
    if not dry_run and root_processed_files:
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

    # Print the run ID
    print(f"{Fore.WHITE}• Run ID: {Fore.GREEN}{run_id}")

    # Query the database to find out how much space was saved this run
    if not dry_run:
        space_saved = query_space_saved(run_id)
        print(f"{Fore.WHITE}• Space saved: {Fore.GREEN}{space_saved / (1024*1024):.2f} MB")

    if not dry_run:
        verify_database()
    
    return success

def query_space_saved(run_id):
    """Query the database to find out how much space was saved this run."""
    # Open the DB connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
            SELECT run_id, 
                   COUNT(*) as file_count, 
                   datetime(MIN(processed_at), 'unixepoch') as start_time,
                   datetime(MAX(processed_at), 'unixepoch') as end_time,
                   SUM(space_saved) as total_saved
            FROM processing_history
            WHERE run_id = ?
    ''', (run_id,))
    result = cursor.fetchone()
    if result:
        return result[4]
    else:
        return 0
    
def process_individual_files(root, files, work_directory, library_path, auto_mode, dry_run, max_threads=0):
    """Process individual CBZ files directly"""
    cbz_files = [f for f in files if f.endswith('.cbz')]
    
    if not cbz_files:
        return False, []
    
    processed_files = []
    success = True

    # Create !Finished directory if it's the root directory
    finished_dir = None
    if root == download_directory and not dry_run:
        finished_dir = os.path.join(download_directory, "!Finished")
        os.makedirs(finished_dir, exist_ok=True)

    for cbz_file in cbz_files:
        filename = cbz_file
        try:
            # Always call match_best_pattern for pattern matching, even in dry run mode
            match_result = match_best_pattern(filename, auto_mode)
            if match_result is None:
                print(f"{Fore.YELLOW}    ⚠ Skipping: {filename} (no match)")
                continue
                
            match_type, match_dict = match_result
            
            print(f"\n{Fore.WHITE}    📄 {'Would process' if dry_run else 'Processing'}: {Fore.YELLOW}{filename}")
            print(f"{Fore.WHITE}    📋 Match type: {Fore.CYAN}{match_type}")
            
            if not match_dict or (not match_dict.get('Title') and not match_dict.get('Series')):
                print(f"{Fore.RED}    ❌ Could not determine series")
                continue
            
            series = match_dict.get('Title') or match_dict.get('Series')
            destination = os.path.join(library_path, series)
            
            file_name = series
            if match_dict.get('Volume'):
                file_name += f" v{match_dict.get('Volume')}"
            if match_dict.get('Chapter'):
                file_name += f" - Chapter {match_dict.get('Chapter')}"
            file_name += ".cbz"
            
            dest_path = os.path.join(destination, file_name)
            
            if dry_run:
                print(f"{Fore.WHITE}    ➡️  : {Fore.GREEN}{dest_path}")
                
                if os.path.exists(destination) and os.path.exists(dest_path):
                    print(f"{Fore.YELLOW}    ⚠️  File already exists at destination")
                
                # Even in dry run, add to processed files to indicate success
                processed_files.append(cbz_file)
            else:
                # Process the CBZ file (actual operations)
                result = process_cbz_file(os.path.join(root, cbz_file), work_directory, library_path, match_dict, max_threads)
                if result:
                    processed_files.append(cbz_file)
                    print(f"{Fore.GREEN}    ✓ Successfully processed")
                    
                    # Move to !Finished immediately if this is a root directory file
                    if finished_dir:
                        source_path = os.path.join(root, cbz_file)
                        if os.path.exists(source_path):
                            try:
                                dest_path = os.path.join(finished_dir, cbz_file)
                                shutil.move(source_path, dest_path)
                                print(f"{Fore.GREEN}    ✓ Moved to !Finished: {cbz_file}")
                                logger.info(f"Moved processed root file to !Finished: {cbz_file}")
                            except Exception as e:
                                print(f"{Fore.RED}    ✘ Error moving file to !Finished: {str(e)}")
                                logger.error(f"Error moving file {cbz_file} to !Finished: {str(e)}")
                else:
                    print(f"{Fore.RED}    ✘ Processing failed")
                success = success and result
        except Exception as e:
            print(f"{Fore.RED}    ❌ Error processing {filename}: {str(e)}")
            success = False
    
    return success, processed_files

def process_bulk_archives(root, files, work_directory, library_path, auto_mode, dry_run, max_threads=0):
    """Process RAR files containing multiple CBZs"""
    rar_files = [f for f in files if f.endswith('.rar')]
    
    if not rar_files:
        logger.info("No RAR files found to process in bulk mode")
        return False, []  # No RAR files to process, not successful
    
    if not can_process_rar:
        logger.warning("Skipping RAR files - unrar not installed")
        print(f"{Fore.YELLOW}⚠ Skipping {len(rar_files)} RAR files - unrar not installed")
        return False, []
    
    success = True
    files_processed = False
    processed_files = []
    
    print(f"\n{Fore.WHITE}Found {Fore.CYAN}{len(rar_files)} RAR files {Fore.WHITE}in {Fore.YELLOW}{os.path.basename(root)} {Fore.WHITE}(bulk mode)")
    
    if dry_run:
        # In dry run mode, we'll examine RAR contents without extraction
        print(f"{Fore.YELLOW}DRY RUN: Examining RAR contents for pattern matching")
        
        # For each RAR file, list contents and identify CBZ files that are inside
        for rar_file in rar_files:
            print(f"\n{Fore.WHITE}  📦 Examining: {Fore.YELLOW}{rar_file}")
            rar_path = os.path.join(root, rar_file)
            
            # Use unrar to list contents without extracting
            try:
                result = subprocess.run(['unrar', 'l', rar_path], 
                                    capture_output=True, text=True, check=False)
                
                # Parse output to find CBZ files
                output_lines = result.stdout.splitlines()
                cbz_files = []
                
                # Extract filenames from unrar output
                for line in output_lines:
                    if line.strip().lower().endswith('.cbz'):
                        # Extract the filename from the line
                        parts = line.strip().split()
                        if len(parts) >= 5:  # unrar list format typically has columns
                            # The filename is usually the last part(s)
                            filename = ' '.join(parts[4:]) if len(parts) > 5 else parts[4]
                            cbz_files.append(filename)
                
                if cbz_files:
                    print(f"{Fore.WHITE}  🔍 Found {Fore.CYAN}{len(cbz_files)} CBZ files {Fore.WHITE}inside:")
                    
                    # Test pattern matching on actual CBZ files inside the RAR
                    for cbz_file in cbz_files:
                        print(f"\n{Fore.WHITE}    📄 Analyzing file: {Fore.CYAN}{cbz_file}")
                        
                        # Run real pattern matching on actual filename
                        match_result = match_best_pattern(cbz_file, auto_mode)
                        if match_result is None:
                            print(f"{Fore.YELLOW}      ⚠ No pattern match for this file")
                            continue
                            
                        match_type, match_dict = match_result
                        
                        print(f"{Fore.WHITE}    📋 Match type: {Fore.CYAN}{match_type}")
                        
                        if match_dict:
                            series = match_dict.get('Title') or match_dict.get('Series')
                            if series:
                                print(f"{Fore.WHITE}    📚 Series: {Fore.GREEN}{series}")
                                
                                # If a match was chosen interactively, it's already stored in the database
                                if match_type not in ["DB_Manual", "None"]:
                                    print(f"{Fore.GREEN}    ✓ Pattern saved to database for future processing")
                else:
                    print(f"{Fore.YELLOW}  ⚠ No CBZ files found inside this RAR archive")
            except Exception as e:
                print(f"{Fore.RED}  ❌ Error examining RAR contents: {str(e)}")
                print(f"{Fore.YELLOW}  ⚠ Falling back to filename-based simulation:")
                
                # Fallback to simulation if listing fails (rare, but possible)
                rar_basename = os.path.splitext(rar_file)[0]
                simulated_files = [f"{rar_basename}.cbz"]
                
                for sim_file in simulated_files:
                    print(f"{Fore.WHITE}    📄 Testing with simulated filename: {Fore.YELLOW}{sim_file}")
                    match_best_pattern(sim_file, auto_mode)
            
            processed_files.append(rar_file)  # Count as processed for dry run stats
            
        return True, processed_files
    else:
        # Extract the RAR files first
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
                        print(f"\n{Fore.WHITE}    📄 Processing extracted: {Fore.YELLOW}{filename}")
                        print(f"{Fore.WHITE}    📋 Match type: {Fore.CYAN}{match_type}")
                        
                        if not match_dict or (not match_dict.get('Title') and not match_dict.get('Series')):
                            print(f"{Fore.RED}    ❌ Could not determine series for {filename}")
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
        
        # Clean up extraction folders and mark processed files
        if success and files_processed:
            processed_files = rar_files.copy()

        return success and files_processed, processed_files

def convert_to_webp(source_filepath, temp_dir, max_threads=0):
    """Convert images in a CBZ file to WebP format and create a new CBZ."""
    try:
        # Count files in original archive first
        original_file_count = 0
        with zipfile.ZipFile(source_filepath, 'r') as zip_ref:
            original_file_count = len(zip_ref.namelist())
            zip_ref.extractall(temp_dir)
        
        # Get a list of all files in the temp directory
        all_files = []
        for root, _, files in os.walk(temp_dir):
            for f in files:
                all_files.append(os.path.join(root, f))
        
        # Check if there are any files to process
        if not all_files:
            logger.error(f"No files found after extracting {source_filepath}")
            return None
            
        # Categorize files by type
        image_files_to_convert = []  # JPG, JPEG, PNG files to convert
        webp_files = []              # Existing WebP files
        other_files = []             # Other files to preserve
        
        for file_path in all_files:
            lower_ext = os.path.splitext(file_path)[1].lower()
            if lower_ext in ('.jpg', '.jpeg', '.png'):
                image_files_to_convert.append(file_path)
            elif lower_ext == '.webp':
                webp_files.append(file_path)
            else:
                other_files.append(file_path)
        
        print(f"{Fore.WHITE}        • Found {Fore.CYAN}{len(image_files_to_convert)} images to convert, "
              f"{Fore.CYAN}{len(webp_files)} existing WebP files, and "
              f"{Fore.CYAN}{len(other_files)} other files")
        
        # Create new WebP archive with no compression
        new_filepath = os.path.join(temp_dir, os.path.basename(source_filepath).replace('.cbz', '_webp.cbz'))
        
        # Initialize counters outside the conditional blocks
        successful_conversions = 0
        failed_conversions = []
        total_files_added = 0
        
        with zipfile.ZipFile(new_filepath, 'w', compression=zipfile.ZIP_STORED) as new_zip:
            # Function to convert a single image
            def convert_image(img_path):
                try:
                    with Image.open(img_path) as img:
                        # Check for dimensions exceeding WebP limits (16383 × 16383)
                        if img.width > 16383 or img.height > 16383:
                            logger.warning(f"Image too large for WebP conversion: {img_path} ({img.width}x{img.height})")
                            return None, img_path, "Image dimensions exceed WebP limit (16383×16383)"
                        
                        webp_path = os.path.splitext(img_path)[0] + '.webp'
                        img.save(webp_path, 'WEBP', quality=75)
                        return webp_path, None, None
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error converting image {img_path}: {error_msg}")
                    return None, img_path, error_msg
            
            # Configure thread count
            if max_threads <= 0:
                import multiprocessing
                cpu_count = multiprocessing.cpu_count()
                max_workers = max(1, cpu_count // 2)
                print(f"{Fore.WHITE}        • Using {Fore.CYAN}auto-configured {max_workers} threads "
                      f"{Fore.WHITE}(50% of {cpu_count} cores)")
            else:
                max_workers = max_threads
                print(f"{Fore.WHITE}        • Using {Fore.CYAN}{max_workers} threads {Fore.WHITE}for conversion")
            
            # Set up progress tracking
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold cyan]{task.description}"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn()
            ) as progress:
                # Convert images if needed
                if image_files_to_convert:
                    convert_task = progress.add_task(
                        "Converting images to WebP", 
                        total=len(image_files_to_convert)
                    )
                    
                    # Convert images using thread pool
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [executor.submit(convert_image, img_path) for img_path in image_files_to_convert]
                        
                        for future in as_completed(futures):
                            webp_path, original_path, error = future.result()
                            progress.update(convert_task, advance=1)
                            
                            if webp_path:
                                # Add the converted file to the new archive
                                rel_path = os.path.relpath(webp_path, temp_dir)
                                new_zip.write(webp_path, rel_path)
                                successful_conversions += 1
                                total_files_added += 1
                            elif original_path:
                                # Conversion failed - add original file to archive
                                rel_path = os.path.relpath(original_path, temp_dir)
                                new_zip.write(original_path, rel_path)
                                failed_conversions.append((original_path, error))
                                total_files_added += 1
                    
                    print(f"{Fore.WHITE}        • {Fore.GREEN}Converted {successful_conversions} "
                          f"{Fore.WHITE}of {Fore.CYAN}{len(image_files_to_convert)} {Fore.WHITE}images")
                    
                    if failed_conversions:
                        print(f"{Fore.WHITE}        • {Fore.YELLOW}Added {len(failed_conversions)} original images "
                              f"{Fore.WHITE}due to conversion failures")
                        # Log the first few failures with reasons
                        for i, (path, error) in enumerate(failed_conversions[:3]):
                            filename = os.path.basename(path)
                            print(f"{Fore.WHITE}          ↳ {Fore.YELLOW}{filename}: {error}")
                        if len(failed_conversions) > 3:
                            print(f"{Fore.WHITE}          ↳ {Fore.YELLOW}and {len(failed_conversions) - 3} more...")
                
                # Add existing WebP files to the archive
                if webp_files:
                    webp_task = progress.add_task(
                        "Adding existing WebP files", 
                        total=len(webp_files)
                    )
                    
                    for webp_file in webp_files:
                        rel_path = os.path.relpath(webp_file, temp_dir)
                        new_zip.write(webp_file, rel_path)
                        progress.update(webp_task, advance=1)
                        total_files_added += 1
                    
                    print(f"{Fore.WHITE}        • {Fore.GREEN}Added {len(webp_files)} existing WebP files "
                          f"{Fore.WHITE}to the archive")
                
                # Add other files to preserve (metadata, etc.)
                if other_files:
                    other_task = progress.add_task(
                        "Adding other files", 
                        total=len(other_files)
                    )
                    
                    for other_file in other_files:
                        rel_path = os.path.relpath(other_file, temp_dir)
                        new_zip.write(other_file, rel_path)
                        progress.update(other_task, advance=1)
                        total_files_added += 1
                    
                    print(f"{Fore.WHITE}        • {Fore.GREEN}Added {len(other_files)} other files "
                          f"{Fore.WHITE}to the archive")
        
        # Verify we added files to the archive
        if total_files_added == 0:
            print(f"{Fore.RED}        ❌ No files were added to the new archive!")
            return None
            
        # Verify file count integrity
        with zipfile.ZipFile(new_filepath, 'r') as zip_ref:
            final_file_count = len(zip_ref.namelist())
            
        if final_file_count != original_file_count:
            print(f"{Fore.RED}        ❌ File count mismatch: Original had {original_file_count} files, "
                  f"but new archive has {final_file_count} files")
            logger.error(f"File count mismatch in WebP conversion: Original={original_file_count}, "
                         f"New={final_file_count} for {source_filepath}")
            return None
        else:
            print(f"{Fore.GREEN}        ✓ File count validated: {original_file_count} files")
        
        print(f"{Fore.WHITE}        • {Fore.GREEN}Created WebP archive {Fore.WHITE}with:")
        print(f"{Fore.WHITE}          • {Fore.GREEN}{successful_conversions} WebP converted images")
        print(f"{Fore.WHITE}          • {Fore.YELLOW}{len(failed_conversions)} original images (conversion failed)")
        print(f"{Fore.WHITE}          • {Fore.CYAN}{len(webp_files)} existing WebP files")
        print(f"{Fore.WHITE}          • {Fore.CYAN}{len(other_files)} other files")
        print(f"{Fore.WHITE}          • {Fore.GREEN}{total_files_added} total files")
        
        # Final size check - reject suspiciously small files
        if os.path.getsize(new_filepath) < 1024:  # Smaller than 1KB is probably corrupt
            print(f"{Fore.RED}        ❌ New archive is suspiciously small ({os.path.getsize(new_filepath)} bytes)")
            return None
            
        return new_filepath
    except Exception as e:
        print(f"{Fore.RED}        ❌ Error converting to WebP: {str(e)}")
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
            elif new_size <= original_size * 1.02:  # Allow up to 2% increase
                print(f"{Fore.YELLOW}      ⚠ WebP size is similar to original (within 2%), using WebP version anyway")
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
        pattern_name = match_dict.get('_pattern_name')
        
        # Special handling for one-shots - check pattern name specifically
        is_oneshot = pattern_name == 'Series_Oneshot_Year'
        
        dest_info = f"{series}"
        if volume:
            dest_info += f" v{volume}"
        if is_oneshot:
            dest_info += " - One-shot"
        elif chapter:
            dest_info += f" - Chapter {chapter}"
            
        print(f"{Fore.WHITE}      • Moving to library: {Fore.GREEN}{dest_info}")

        # Pass the is_oneshot flag to move_to_library
        dest_path = move_to_library(file_to_move, library_path, series, volume, chapter, is_oneshot=is_oneshot)
        if dest_path:
            print(f"{Fore.GREEN}      ✓ File moved successfully to library")
        else:
            print(f"{Fore.YELLOW}      ⚠ File move completed with warnings")

        record_processing_history(
            source_path=filepath,
            dest_path=dest_path,
            original_size=original_size,
            final_size=os.path.getsize(dest_path) if os.path.exists(dest_path) else None,
            run_id=run_id
        )

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

def move_to_library(source_file, library_path, series_name, volume=None, chapter=None, download_directory=None, is_oneshot=False):
    # Before we create the filename, validate the volume/chapter information
    # Force detect volume information if it's in the filename but not in the match_dict
    source_filename = os.path.basename(source_file)
    if not volume and re.search(r'\bv(?:ol)?\.?\s*(\d+)\b', source_filename, re.IGNORECASE):
        vol_match = re.search(r'\bv(?:ol)?\.?\s*(\d+)\b', source_filename, re.IGNORECASE)
        volume = vol_match.group(1)
        logger.warning(f"Volume information forced from filename: v{volume} (missing in pattern match)")
        print(f"{Fore.YELLOW}    ⚠ Volume information extracted directly from filename: v{volume}")
        
        # If the chapter is a year (like 2023), it's probably incorrect - clear it
        if chapter and chapter.isdigit() and int(chapter) > 1900 and int(chapter) < 2100:
            if re.search(r'\(\d{4}\)', source_filename):  # Confirm it's likely a year in parentheses
                logger.warning(f"Cleared likely incorrect chapter number (year): {chapter}")
                print(f"{Fore.YELLOW}    ⚠ Cleared likely incorrect chapter number (year): {chapter}")
                chapter = None
    
    series_path = os.path.join(library_path, series_name)

    if not os.path.exists(series_path):
        os.makedirs(series_path)

    file_name = series_name
    if volume:
        file_name += f" v{volume}"
    if is_oneshot:
        file_name += " - One-shot"
    elif chapter:  # Only add chapter if not a one-shot
        file_name += f" - Chapter {chapter}"
    file_name += os.path.splitext(source_file)[1]

    dest_path = os.path.join(series_path, file_name)
    print(f"{Fore.WHITE}        • Destination: {Fore.CYAN}{dest_path}")

    # Check if source is directly in process directory (affects copy vs move behavior)
    source_dir = os.path.dirname(source_file)
    is_root_file = os.path.samefile(source_dir, os.path.dirname(os.path.abspath(source_file)))

    if os.path.exists(dest_path):
        # Handle file already exists cases
        if re.search(r'\(F\d?\)', os.path.basename(source_file)):
            os.remove(dest_path)
            shutil.move(source_file, dest_path)
            print(f"{Fore.GREEN}        ✓ Replaced existing file with fixed version")
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
                counter += 1
            print(f"{Fore.YELLOW}        ⚠ File already exists. Moving to: {Fore.CYAN}{conflict_dest_path}")
            dest_path = conflict_dest_path
        else:
            base, ext = os.path.splitext(dest_path)
            counter = 1
            while os.path.exists(dest_path):
                dest_path = f"{base}_{counter}{ext}"
                counter += 1
            print(f"{Fore.YELLOW}        ⚠ File already exists. Saving as: {Fore.CYAN}{dest_path}")
    
    # Copy from root directory, move from subdirectories
    if is_root_file:
        shutil.copy2(source_file, dest_path)
        logger.debug(f"Copied {source_file} to {dest_path}")
    else:
        shutil.move(source_file, dest_path)
        logger.debug(f"Moved {source_file} to {dest_path}")
    
    return dest_path

def record_processing_history(source_path, dest_path, original_size, final_size, processed_at=None, run_id=None):
    """Record a processed file in the processing_history table."""
    db_path = get_db_path()

    if processed_at is None:
        processed_at = int(time.time())
    space_saved = original_size - final_size if original_size and final_size else None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO processing_history (run_id, source_path, dest_path, original_size, final_size, space_saved, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (run_id, source_path, dest_path, original_size, final_size, space_saved, processed_at))
        conn.commit()
        logger.debug(f"Recorded processing history for run_id: {run_id}, source_path: {source_path}")
    except Exception as e:
        logger.error(f"Failed to record processing history: {e}")
    finally:
        conn.close()

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
                match_dict['_pattern_name'] = pattern_name
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

def store_pattern_match(series_name, pattern_name, is_manual=False, raw_match=None):
    """Store a pattern match for a series in the database.
    
    Args:
        series_name: Name of the series
        pattern_name: Name of the pattern that works for this series
        is_manual: Whether this was a manually entered pattern
        raw_match: Optional raw match data for reference (not used for matching)
    """
    if not series_name:
        logger.warning(f"Attempted to store pattern with empty series name")
        return False
        
    # Normalize series name
    series_name = series_name.strip()
    
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    current_time = int(time.time())
    
    logger.debug(f"Storing pattern for '{series_name}' - pattern: {pattern_name} (manual: {is_manual})")
    
    try:
        # Store match_dict as JSON string if provided
        match_json = None
        if raw_match:
            import json
            match_json = json.dumps(raw_match)
            
        # Check if series already exists
        cursor.execute('SELECT id, use_count FROM series_patterns WHERE series_name = ?', (series_name,))
        result = cursor.fetchone()
        
        if result:
            # Update existing entry
            series_id, use_count = result
            cursor.execute('''
                UPDATE series_patterns 
                SET pattern_name = ?, match_dict = ?, last_used_at = ?,
                    use_count = ?, is_manual = ?
                WHERE id = ?
            ''', (pattern_name, match_json, current_time, use_count + 1, 1 if is_manual else 0, series_id))
            logger.debug(f"Updated pattern for series: {series_name}")
        else:
            # Insert new entry
            cursor.execute('''
                INSERT INTO series_patterns 
                (series_name, pattern_name, match_dict, created_at, last_used_at, is_manual)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (series_name, pattern_name, match_json, current_time, current_time, 1 if is_manual else 0))
            logger.debug(f"Saved new pattern for series: {series_name}")
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Database error storing pattern: {e}")
        return False
    finally:
        conn.close()

def get_stored_pattern(filename):
    """Get the stored pattern for a file based on its series name."""
    # Extract potential series name from filename
    series_name = extract_series_name(filename)
    if not series_name:
        return None
        
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    try:
        # Include match_dict in the query
        cursor.execute('''
            SELECT pattern_name, is_manual, use_count, match_dict
            FROM series_patterns
            WHERE series_name = ?
        ''', (series_name,))
        
        result = cursor.fetchone()
        
        # If no exact match, try prefix matching
        if not result:
            cursor.execute('''
                SELECT pattern_name, is_manual, series_name, use_count, match_dict
                FROM series_patterns
                WHERE ? LIKE (series_name || '%')
                ORDER BY LENGTH(series_name) DESC
                LIMIT 1
            ''', (filename,))
            result = cursor.fetchone()
        
        if not result:
            return None
            
        pattern_info = {}
        
        if len(result) == 4:  # Exact match
            pattern_info['pattern_name'], pattern_info['is_manual'], pattern_info['use_count'], match_json = result
            pattern_info['series_name'] = series_name
        else:  # Prefix match
            pattern_info['pattern_name'], pattern_info['is_manual'], pattern_info['series_name'], pattern_info['use_count'], match_json = result
            
        # Parse JSON match_dict if available
        if match_json:
            import json
            try:
                pattern_info['match_dict'] = json.loads(match_json)
                # Always ensure pattern name is stored in match_dict
                pattern_info['match_dict']['_pattern_name'] = pattern_info['pattern_name']
            except json.JSONDecodeError:
                logger.warning(f"Could not parse match_dict JSON for {series_name}")
                pattern_info['match_dict'] = {'_pattern_name': pattern_info['pattern_name']}
        else:
            pattern_info['match_dict'] = {'_pattern_name': pattern_info['pattern_name']}
            
        # Update usage stats
        current_time = int(time.time())
        new_use_count = pattern_info['use_count'] + 1
        cursor.execute('''
            UPDATE series_patterns
            SET last_used_at = ?, use_count = ?
            WHERE series_name = ?
        ''', (current_time, new_use_count, pattern_info['series_name']))
        conn.commit()
        
        # Update the count to reflect the new value after increment
        pattern_info['use_count'] = new_use_count
        
        return pattern_info
    finally:
        conn.close()

def extract_series_name(filename):
    """Extract series name from filename using best-effort approach.
    
    Returns the most likely series name from the filename.
    """
    # Try all our patterns to extract the series name
    for pattern_name, pattern in patterns:
        match = pattern.match(filename)
        if match:
            match_dict = match.groupdict()
            match_dict['_pattern_name'] = pattern_name
            score = score_match(match_dict, filename)
            if score > 0:  # Only consider positive scores
                series = match_dict.get('Title') or match_dict.get('Series')
                if series:
                    return series.strip()
    
    # Fallback if no pattern matched well
    # Just take everything before the first numeric that could be a chapter
    parts = re.split(r'\s+\d+', filename)
    if len(parts) > 1:
        return parts[0].strip()
    
    # If all else fails, just take the first part of the filename
    parts = filename.split(' ')
    return ' '.join(parts[:min(3, len(parts))]).strip()

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
    print(f"{Fore.YELLOW}  • /tmp")
    
    while True:
        print()
        use_default = input(f"{Fore.GREEN}➤ Use default location? (y/n): {Fore.WHITE}").strip().lower()

        if use_default == 'y':
            base_dir = default_dir
        elif use_default == 'n':
            base_dir = input(f"{Fore.GREEN}➤ Enter base work directory path: {Fore.WHITE}")
        else:
            print(f"{Fore.RED}✘ Invalid option. Please enter 'y' or 'n'.")
            continue

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
    print(f"{Fore.YELLOW}  • /home/user/Comics")
    
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
    print(f"{Fore.YELLOW}  • /home/user/Downloads")
    
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

def confirm_processing(download_directory, library_path, work_directory, dry_run=False, process_mode="auto", max_threads=0):
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
    print(f"{Fore.WHITE}4. Use temporary work directory: {Fore.YELLOW}{work_directory}")
    print(f"{Fore.WHITE}5. Use processing mode: {Fore.YELLOW}{mode_desc}")
    
    if dry_run:
        print(f"\n{Fore.YELLOW}⚠ DRY RUN MODE: No files will be modified")
    
    print(f"\n{Fore.WHITE}During processing:")
    if max_threads == 0:
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        max_workers = max(1, cpu_count // 2)
        print(f"{Fore.WHITE}• Files will be converted to WebP format when beneficial using{Fore.YELLOW} {max_workers} threads")
    else:
        print(f"{Fore.WHITE}• Files will be converted to WebP format when beneficial using{Fore.YELLOW} {max_threads} threads")
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

def get_potential_series_name(filename):
    """Extract series name using existing pattern matching infrastructure"""
    # First try all patterns to see if any match with high confidence
    for pattern_name, pattern in patterns:
        match = pattern.match(filename)
        if match and match.groupdict():
            match_dict = match.groupdict()
            match_dict['_pattern_name'] = pattern_name
            score = score_match(match_dict, filename)
            
            # Only use high-confidence matches
            if score > 5:
                series = match_dict.get('Title') or match_dict.get('Series')
                if series:
                    return series
    
    # Fall back to the original simple approach if no good match
    parts = filename.split(' ')
    return ' '.join(parts[:min(3, len(parts))]).strip()

def list_recent_runs():
    """List recent processing runs with summary information"""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get distinct run_ids with summary info
        cursor.execute('''
            SELECT run_id, 
                   COUNT(*) as file_count, 
                   datetime(MIN(processed_at), 'unixepoch') as start_time,
                   datetime(MAX(processed_at), 'unixepoch') as end_time,
                   SUM(space_saved) as total_saved
            FROM processing_history
            GROUP BY run_id
            ORDER BY MAX(processed_at) DESC
            LIMIT 10
        ''')
        
        runs = cursor.fetchall()
        
        if not runs:
            print(f"{Fore.YELLOW}No processing runs found in database.")
            return None
            
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.CYAN}📋 RECENT PROCESSING RUNS")
        print(f"{Fore.CYAN}{'='*60}")
        
        for i, (run_id, file_count, start_time, end_time, space_saved) in enumerate(runs, 1):
            # Show more friendly date formatting
            run_date = start_time.split()[0]
            run_time = start_time.split()[1].split('.')[0]  # Remove microseconds
            
            print(f"\n{Fore.WHITE}{i}. {Fore.YELLOW}Run ID: {run_id[:8]}...")
            print(f"   {Fore.WHITE}Date: {Fore.GREEN}{run_date} at {run_time}")
            print(f"   {Fore.WHITE}Files Processed: {Fore.GREEN}{file_count}")
            
            if space_saved:
                saved_mb = space_saved / (1024*1024)
                print(f"   {Fore.WHITE}Space Saved: {Fore.GREEN}{saved_mb:.2f} MB")
        
        return runs
    except Exception as e:
        print(f"{Fore.RED}Error getting run history: {str(e)}")
        return None
    finally:
        conn.close()

def undo_processing_run(run_id):
    """Undo a processing run by removing files from the library and restoring original folder structure"""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get all files processed in this run
        cursor.execute('''
            SELECT id, source_path, dest_path
            FROM processing_history
            WHERE run_id = ?
        ''', (run_id,))
        
        files = cursor.fetchall()
        
        if not files:
            print(f"{Fore.YELLOW}No files found for run ID: {run_id}")
            return False
        
        # Show summary of what will be undone
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.CYAN}⚠️ UNDO PROCESSING RUN")
        print(f"{Fore.CYAN}{'='*60}")
        
        print(f"\n{Fore.WHITE}This will remove {Fore.YELLOW}{len(files)} files {Fore.WHITE}from your library.")
        print(f"{Fore.WHITE}It will also restore folders and files from the !Finished directory.")
        
        # Show sample of files to be removed
        sample_size = min(10, len(files))
        print(f"\n{Fore.WHITE}Sample of files to be removed:")
        for i in range(sample_size):
            _, _, dest_path = files[i]
            print(f"  {Fore.RED}• {os.path.basename(dest_path)}")
        
        if len(files) > sample_size:
            print(f"  {Fore.WHITE}... and {len(files) - sample_size} more files")
        
        # Confirm before proceeding
        confirm = input(f"\n{Fore.RED}⚠️ This action cannot be undone. Continue? (y/n): {Fore.WHITE}").strip().lower()
        if confirm != 'y':
            print(f"{Fore.YELLOW}Undo operation cancelled.")
            return False
        
        # Identify source directories and possible download directories
        source_dirs = set()
        download_dirs = set()
        source_paths_by_dir = {}
        
        for file_id, source_path, _ in files:
            if not source_path:
                continue
                
            source_dir = os.path.dirname(source_path)
            if source_dir:
                source_dirs.add(source_dir)
                
                # Group source paths by directory for folder restoration
                if source_dir not in source_paths_by_dir:
                    source_paths_by_dir[source_dir] = []
                source_paths_by_dir[source_dir].append(source_path)
                
                # Add potential download directories
                parent_dir = os.path.dirname(source_dir)
                if parent_dir:
                    download_dirs.add(parent_dir)
        
        # Create recovery folder for files that can't be restored to original location
        recovery_dir = None
        if download_dirs:
            main_download_dir = sorted(download_dirs, key=len)[0]  # Use shortest path as main
            recovery_dir = os.path.join(main_download_dir, f"!Recovered_{run_id[:8]}")
            os.makedirs(recovery_dir, exist_ok=True)
            print(f"{Fore.CYAN}Created recovery directory: {recovery_dir}")
        else:
            # If no download dirs found, create recovery in same directory as script
            script_dir = os.path.dirname(os.path.abspath(__file__))
            recovery_dir = os.path.join(script_dir, f"!Recovered_{run_id[:8]}")
            os.makedirs(recovery_dir, exist_ok=True)
            print(f"{Fore.CYAN}Created recovery directory: {recovery_dir}")
        
        # Remove files from library
        print(f"\n{Fore.CYAN}Removing files from library...")
        removed_count = 0
        failed_count = 0
        
        for file_id, source_path, dest_path in files:
            try:
                if os.path.exists(dest_path):
                    os.remove(dest_path)
                    removed_count += 1
                    # Mark as undone in database
                    cursor.execute('''
                        UPDATE processing_history
                        SET undone_at = ?
                        WHERE id = ?
                    ''', (int(time.time()), file_id))
                else:
                    print(f"{Fore.YELLOW}⚠️ File not found: {dest_path}")
                    failed_count += 1
            except Exception as e:
                print(f"{Fore.RED}✘ Error removing {dest_path}: {str(e)}")
                failed_count += 1
        
        # Clean up empty series directories
        print(f"\n{Fore.CYAN}Cleaning up empty series directories...")
        cleaned_dirs = 0
        
        # Get unique directories from destination paths
        series_dirs = set(os.path.dirname(file[2]) for file in files)
        
        for series_dir in series_dirs:
            try:
                if os.path.exists(series_dir) and not os.listdir(series_dir):
                    os.rmdir(series_dir)
                    cleaned_dirs += 1
            except Exception as e:
                print(f"{Fore.YELLOW}⚠️ Could not remove directory {series_dir}: {str(e)}")
        
        # First attempt to restore entire folders
        print(f"\n{Fore.CYAN}Restoring folders from !Finished directory...")
        restored_folders = set()
        folder_recovery_count = 0
        
        # Identify potential processed folders in the !Finished directory
        potential_folders = {}
        for download_dir in download_dirs:
            finished_dir = os.path.join(download_dir, "!Finished")
            if os.path.exists(finished_dir):
                for item in os.listdir(finished_dir):
                    item_path = os.path.join(finished_dir, item)
                    if os.path.isdir(item_path):
                        # Store path and parent directory for reference
                        potential_folders[item] = {
                            'path': item_path,
                            'parent': download_dir
                        }
        
        # Try to match and restore folders
        for source_dir in source_dirs:
            dir_name = os.path.basename(source_dir)
            parent_dir = os.path.dirname(source_dir)
            
            # Skip if this folder has already been restored
            if source_dir in restored_folders:
                continue
                
            # Check if the folder exists in the !Finished directory
            if dir_name in potential_folders:
                finished_folder = potential_folders[dir_name]
                # Only restore if the parent directories match
                expected_parent = finished_folder['parent']
                
                if parent_dir == expected_parent:
                    try:
                        if os.path.exists(source_dir):
                            print(f"{Fore.YELLOW}⚠️ Destination folder already exists: {source_dir}")
                            temp_name = f"{source_dir}_recovered_{run_id[:8]}"
                            shutil.move(finished_folder['path'], temp_name)
                            print(f"{Fore.YELLOW}  ↳ Restored to alternate location: {temp_name}")
                        else:
                            # Create parent directory if it doesn't exist
                            os.makedirs(parent_dir, exist_ok=True)
                            # Restore the entire folder structure
                            shutil.move(finished_folder['path'], source_dir)
                            print(f"{Fore.GREEN}✓ Restored folder: {dir_name} to original location")
                            
                        restored_folders.add(source_dir)
                        folder_recovery_count += 1
                        
                        # Prevent individual file processing for files in this folder
                        for src_path in source_paths_by_dir.get(source_dir, []):
                            restored_folders.add(src_path)
                        
                    except Exception as e:
                        print(f"{Fore.RED}✘ Error restoring folder {dir_name}: {str(e)}")
        
        # Process individual files that couldn't be restored with folders
        print(f"\n{Fore.CYAN}Recovering individual files from !Finished folder...")
        recovered_count = 0
        recovery_count = 0
        
        for file_id, source_path, dest_path in files:
            # Skip if already handled as part of a folder restoration
            if source_path in restored_folders:
                continue
                
            # Get filename and try to find it in !Finished folders
            filename = os.path.basename(source_path)
            source_dir = os.path.dirname(source_path)
            
            # Check if source directory exists
            if not os.path.exists(source_dir):
                source_dir = None
            
            # First, try exact !Finished location
            finished_path = None
            
            # Check for file in !Finished folder in same directory as source
            if source_dir:
                finished_dir = os.path.join(os.path.dirname(source_dir), "!Finished")
                if os.path.exists(finished_dir):
                    # Check both for the file and any containing folder with that file
                    possible_file = os.path.join(finished_dir, filename)
                    if os.path.exists(possible_file) and os.path.isfile(possible_file):
                        finished_path = possible_file
                    
                    # Check if the file might be in a subfolder of !Finished
                    if not finished_path:
                        for root, _, files in os.walk(finished_dir):
                            if filename in files:
                                finished_path = os.path.join(root, filename)
                                break
            
            # If not found, try searching in all download directories
            if not finished_path:
                for download_dir in download_dirs:
                    finished_dir = os.path.join(download_dir, "!Finished")
                    if os.path.exists(finished_dir):
                        possible_file = os.path.join(finished_dir, filename)
                        if os.path.exists(possible_file) and os.path.isfile(possible_file):
                            finished_path = possible_file
                            break
                            
                        # Check subfolders too
                        for root, _, files in os.walk(finished_dir):
                            if filename in files:
                                finished_path = os.path.join(root, filename)
                                break
                    
                    if finished_path:
                        break
            
            # If found, try to move it back
            if finished_path:
                try:
                    if source_dir and os.path.exists(source_dir):
                        # Move back to original directory
                        dest_file = os.path.join(source_dir, filename)
                        shutil.move(finished_path, dest_file)
                        print(f"{Fore.GREEN}  ✓ Restored file: {filename} to original location")
                        recovered_count += 1
                    elif recovery_dir:
                        # Move to recovery directory
                        dest_file = os.path.join(recovery_dir, filename)
                        shutil.move(finished_path, dest_file)
                        print(f"{Fore.YELLOW}  ⚠️ Moved to recovery dir: {filename}")
                        recovery_count += 1
                    else:
                        print(f"{Fore.YELLOW}  ⚠️ No recovery directory available for: {filename}")
                except Exception as e:
                    print(f"{Fore.RED}  ✘ Error recovering file {filename}: {str(e)}")
        
        conn.commit()
        
        print(f"\n{Fore.GREEN}{'='*60}")
        print(f"{Fore.GREEN}✅ UNDO COMPLETE")
        print(f"{Fore.GREEN}{'='*60}")
        
        print(f"\n{Fore.WHITE}• Removed {Fore.GREEN}{removed_count} files {Fore.WHITE}from library")
        print(f"{Fore.WHITE}• Cleaned {Fore.GREEN}{cleaned_dirs} empty directories")
        if folder_recovery_count > 0:
            print(f"{Fore.WHITE}• Restored {Fore.GREEN}{folder_recovery_count} folders {Fore.WHITE}to original locations")
        print(f"{Fore.WHITE}• Restored {Fore.GREEN}{recovered_count} individual files {Fore.WHITE}to original locations")
        if recovery_count > 0:
            print(f"{Fore.WHITE}• Moved {Fore.YELLOW}{recovery_count} files {Fore.WHITE}to recovery folder")
        if recovery_dir:
            print(f"{Fore.WHITE}• Recovery folder: {Fore.CYAN}{recovery_dir}")
        if failed_count > 0:
            print(f"{Fore.WHITE}• Failed to remove {Fore.YELLOW}{failed_count} files")
        
        return True
    except Exception as e:
        print(f"{Fore.RED}Error undoing processing run: {str(e)}")
        logger.error(f"Error undoing processing run: {str(e)}", exc_info=True)
        conn.rollback()
        return False
    finally:
        conn.close()

def undo_interactive():
    """Interactive mode for undoing processing runs"""
    runs = list_recent_runs()
    
    if not runs:
        return
    
    while True:
        choice = input(f"\n{Fore.GREEN}➤ Enter number to undo a run (or 'q' to quit): {Fore.WHITE}").strip()
        
        if choice.lower() == 'q':
            return
            
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(runs):
                run_id = runs[idx][0]  # Extract run_id from selected run
                undo_processing_run(run_id)
                return
            else:
                print(f"{Fore.RED}Invalid selection. Please enter a number between 1 and {len(runs)}.")
        except ValueError:
            print(f"{Fore.RED}Please enter a valid number or 'q' to quit.")

def test_database_matches(directory):
    """Test database matches against files in a directory"""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}🔍 DATABASE MATCHING TEST")
    print(f"{Fore.CYAN}{'='*60}")
    
    matched_count = 0
    unmatched_count = 0
    
    print(f"\n{Fore.WHITE}Testing files in: {Fore.YELLOW}{directory}")
    
    # Initialize database if needed
    if not os.path.exists(get_db_path()):
        print(f"{Fore.YELLOW}Database not found. Creating new database.")
        init_database()
    
    # First, show database summary
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM series_patterns")
        pattern_count = cursor.fetchone()[0]
        print(f"{Fore.WHITE}Database contains {Fore.CYAN}{pattern_count} {Fore.WHITE}pattern entries")
        
        # Show first few entries
        if pattern_count > 0:
            cursor.execute("SELECT series_name, pattern_name FROM series_patterns LIMIT 5")
            samples = cursor.fetchall()
            print(f"{Fore.WHITE}Sample entries:")
            for series, pattern in samples:
                print(f"  {Fore.CYAN}{series}{Fore.WHITE} → {Fore.YELLOW}{pattern}")
    except Exception as e:
        print(f"{Fore.RED}Database error: {str(e)}")
    finally:
        conn.close()
    
    for root, _, files in os.walk(directory):
        if any(special in root for special in ["!Finished", "!temp_processing", "!temp_extract"]):
            continue
            
        cbz_files = [f for f in files if f.endswith('.cbz')]
        
        for cbz_file in cbz_files:
            filename = cbz_file
            
            print(f"\n{Fore.WHITE}File: {Fore.YELLOW}{filename}")
            
            # Extract potential series name and show it
            potential_series = get_potential_series_name(filename)
            print(f"{Fore.WHITE}Extracted series: {Fore.CYAN}{potential_series}")
            
            # Check database for a pattern match
            db_match = get_stored_pattern(filename)  # Try with full filename first
            
            if not db_match:
                # If full filename didn't match, try with extracted series name
                db_match = get_stored_pattern(potential_series)
            
            if db_match:
                matched_count += 1
                print(f"{Fore.GREEN}✓ Database match found:")
                print(f"  {Fore.WHITE}Matched with: {Fore.CYAN}{db_match.get('stored_series', potential_series)}")
                print(f"  {Fore.WHITE}Pattern: {Fore.CYAN}{db_match['pattern_name']}")
                print(f"  {Fore.WHITE}Type: {Fore.CYAN}{'Manual' if db_match['is_manual'] else 'Auto'}")
                print(f"  {Fore.WHITE}Uses: {Fore.CYAN}{db_match['use_count']} times")
                
                # Test applying the pattern
                if db_match['is_manual']:
                    # Manual pattern handling
                    if 'match_dict' in db_match and db_match['match_dict']:
                        match_dict = db_match['match_dict'].copy()
                    else:
                        match_dict = {'Series': db_match['series_name']}
                        
                    # Try to extract chapter number
                    chapter_match = re.search(r'\b(\d+(?:\.\d+)?)\b', 
                                        filename[len(db_match['series_name'])+1:])
                    if chapter_match:
                        match_dict['Chapter'] = chapter_match.group(1)
                else:
                    # Find and apply the pattern
                    for pattern_name, pattern in patterns:
                        if pattern_name == db_match['pattern_name']:
                            match = pattern.match(filename)
                            if match and match.groupdict():
                                match_dict = match.groupdict()
                                break
                    else:
                        match_dict = None
                
                if match_dict:
                    print(f"  {Fore.WHITE}Resulting match data:")
                    for k, v in match_dict.items():
                        if k not in ('_pattern_name', '_original_filename'):
                            print(f"    {Fore.WHITE}{k}: {Fore.CYAN}{v}")
                else:
                    print(f"  {Fore.YELLOW}⚠ Found pattern doesn't match current file format")
            else:
                unmatched_count += 1
                print(f"{Fore.YELLOW}⚠ No database match found")
                
                # Ask if user wants to try interactive matching
                choice = input(f"{Fore.GREEN}➤ Try interactive matching? (y/n): {Fore.WHITE}").strip().lower()
                if choice == 'y':
                    match_type, match_dict = match_best_pattern(filename, auto_mode=False)
                    if match_dict:
                        print(f"{Fore.GREEN}✓ Match created and stored in database")
    
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}📊 MATCHING SUMMARY")
    print(f"{Fore.CYAN}{'='*60}")
    print(f"{Fore.WHITE}Total files checked: {Fore.GREEN}{matched_count + unmatched_count}")
    print(f"{Fore.WHITE}Files with database matches: {Fore.GREEN}{matched_count}")
    print(f"{Fore.WHITE}Files without matches: {Fore.YELLOW}{unmatched_count}")
    
    # If we have unmatched files, suggest what to do
    if unmatched_count > 0:
        print(f"\n{Fore.CYAN}💡 TIP: Run with --dry-run to interactively match remaining files")
        print(f"{Fore.CYAN}    without actually processing them. Then run normally.")

def verify_database():
    """Verify database is accessible and show record count"""
    db_path = get_db_path()
    
    if not os.path.exists(db_path):
        print(f"{Fore.RED}Database file not found at {db_path}")
        return False
        
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM series_patterns")
        count = cursor.fetchone()[0]
        print(f"{Fore.GREEN}✓ Database verified: {count} patterns stored")
        conn.close()
        return True
    except Exception as e:
        print(f"{Fore.RED}Database error: {str(e)}")
        return False

def show_database_stats():
    """Display statistics about the pattern database."""
    db_path = get_db_path()

    if not os.path.exists(db_path):
        print(f"{Fore.YELLOW}No pattern database found at {db_path}")
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get total pattern count
        cursor.execute("SELECT COUNT(*) FROM series_patterns")
        total_count = cursor.fetchone()[0]
        
        # Get manual vs automatic patterns
        cursor.execute("SELECT COUNT(*) FROM series_patterns WHERE is_manual = 1")
        manual_count = cursor.fetchone()[0]
        auto_count = total_count - manual_count
        
        # Get most used patterns
        cursor.execute("""
            SELECT series_name, pattern_name, use_count, 
                   datetime(last_used_at, 'unixepoch') as last_used
            FROM series_patterns 
            ORDER BY use_count DESC 
            LIMIT 10
        """)

        top_patterns = cursor.fetchall()
        
        print(f"\n{Fore.CYAN}{'='*60}")
        print(f"{Fore.CYAN}📊 PATTERN DATABASE STATISTICS")
        print(f"{Fore.CYAN}{'='*60}")
        
        print(f"\n{Fore.WHITE}Database location: {Fore.YELLOW}{db_path}")
        print(f"{Fore.WHITE}Total stored patterns: {Fore.GREEN}{total_count}")
        print(f"{Fore.WHITE}Manual patterns: {Fore.GREEN}{manual_count}")
        print(f"{Fore.WHITE}Auto-selected patterns: {Fore.GREEN}{auto_count}")
        
        if top_patterns:
            print(f"\n{Fore.CYAN}Most Used Patterns:")
            for i, (series, pattern, count, last_used) in enumerate(top_patterns, 1):
                print(f"{Fore.WHITE}{i}. {Fore.YELLOW}{series}")
                print(f"   {Fore.WHITE}Pattern: {Fore.BLUE}{pattern}")
                print(f"   {Fore.WHITE}Uses: {Fore.GREEN}{count}")
                print(f"   {Fore.WHITE}Last used: {Fore.GREEN}{last_used}")
        
    except Exception as e:
        print(f"{Fore.RED}Error retrieving database stats: {str(e)}")
    finally:
        conn.close()

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
                        help="Maximum number of threads for image conversion (0=auto, default: 50%% of CPU cores)")
    parser.add_argument("--db-stats", action="store_true",
                        help="Show statistics about the pattern database")
    parser.add_argument("--clear-patterns", action="store_true",
                        help="Clear all stored pattern matches from the database")
    parser.add_argument("--test-db", action="store_true",
                        help="Test database matches against files without processing them")
    parser.add_argument("--undo", action="store_true",
                        help="Undo a previous processing run by removing files from the library")
    return parser.parse_args()


def check_dependencies():
    """Check for required dependencies and libraries"""
    missing_deps = []
    global can_process_rar
    can_process_rar = True
    
    # Check unrar - warn but don't fail if missing
    unrar_path = shutil.which("unrar")
    if unrar_path is None:
        warning_msg = "WARNING: 'unrar' is not installed or not found in the system path."
        print(f"{Fore.YELLOW}⚠ {warning_msg}", file=sys.stderr)
        print(f"{Fore.YELLOW}⚠ RAR files will be skipped during processing.", file=sys.stderr)
        logger.warning(warning_msg)
        can_process_rar = False
        
    # Check Python libraries - these are essential
    required_libs = [
        ("zipfile", "Standard library"),
        ("PIL.Image", "Pillow"),
        ("concurrent.futures", "Standard library"),
        ("rich.progress", "rich"),
        ("colorama", "colorama")
    ]
    
    for lib, package in required_libs:
        try:
            __import__(lib)
        except ImportError:
            error_msg = f"ERROR: Missing required library: {lib} (from package '{package}')"
            missing_deps.append(error_msg)
            print(f"{Fore.RED}✘ {error_msg}", file=sys.stderr)
    
    # If any essential deps are missing, write to log file and exit
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
        if can_process_rar:
            print(f"{Fore.GREEN}✓ All dependencies are installed.")
        else:
            print(f"{Fore.GREEN}✓ Core dependencies are installed. {Fore.YELLOW}(RAR support unavailable)")

def get_db_path():
    """Returns the path to the SQLite database file."""
    # Store in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, 'manga_patterns.db')

def init_database():
    """Initialize the database with required tables."""
    db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS series_patterns (
        id INTEGER PRIMARY KEY,
        series_name TEXT NOT NULL,
        pattern_name TEXT,
        is_manual INTEGER DEFAULT 0,
        match_dict TEXT,
        created_at INTEGER,
        last_used_at INTEGER,
        use_count INTEGER DEFAULT 1,
        UNIQUE(series_name)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS processing_history (
        id INTEGER PRIMARY KEY,
        run_id TEXT,
        source_path TEXT NOT NULL,
        dest_path TEXT NOT NULL,
        original_size INTEGER,
        final_size INTEGER,
        space_saved INTEGER,
        processed_at INTEGER,
        undone_at INTEGER DEFAULT NULL
    )
    ''')

    # Index for fast lookups by series name
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_name ON series_patterns(series_name)')
    
    # Index for processing history
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_run_id ON processing_history(run_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_undone_at ON processing_history(undone_at)')

    conn.commit()
    conn.close()
    
    return db_path

if __name__ == '__main__':
    logger = setup_logging()
    args = parse_arguments()
    
    # Initialize database first
    db_path = init_database()
    
    # Generate unique run ID for this session
    run_id = str(uuid.uuid4())
    
    # Create a visually appealing startup info box
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}🚀 MANGA PROCESSOR STARTUP")
    print(f"{Fore.CYAN}{'='*60}")
    
    # Show version information
    version = "0.9.7.1"
    print(f"\n{Fore.WHITE}Version: {Fore.GREEN}{version}")
    print(f"{Fore.WHITE}Session ID: {Fore.YELLOW}{run_id[:8]}...")
    
    # Database info
    print(f"\n{Fore.WHITE}Database:")
    print(f"{Fore.WHITE}  • Path: {Fore.CYAN}{db_path}")
    
    # Get database stats if available
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM series_patterns")
        pattern_count = cursor.fetchone()[0]
        conn.close()
        print(f"{Fore.WHITE}  • Patterns: {Fore.GREEN}{pattern_count}")
    except Exception as e:
        print(f"{Fore.WHITE}  • Patterns: {Fore.YELLOW}None ({e})")
    
    # Dependency check
    print(f"\n{Fore.WHITE}Checking dependencies:")
    check_dependencies()
    
    # Log the startup information
    logger.info(f"Manga Processor v{version} started with run ID {run_id}")
    logger.info(f"Database initialized at {db_path}")

    if args.db_stats:
        show_database_stats()
        exit(0)
        
    if args.clear_patterns:
        confirm = input(f"{Fore.YELLOW}This will clear ALL stored pattern matches. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM series_patterns")
            conn.commit()
            conn.close()
            print(f"{Fore.GREEN}✓ All pattern matches cleared from database.")
        exit(0)

    if args.test_pattern:
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

    if args.test_db:
        test_database_matches(args.source if args.source else get_download_directory())
        exit(0)

    if args.undo:
        undo_interactive()
        exit(0)

    library_path = args.dest if args.dest else get_library_path()
    download_directory = args.source if args.source else get_download_directory()
    work_directory = args.work_dir if args.work_dir else get_work_directory()

    if not args.auto:
        if not confirm_processing(download_directory, library_path, work_directory, args.dry_run, args.mode, args.threads):
            print(f"{Fore.YELLOW}Exiting without processing.")
            exit(0)

    process_directory(download_directory, library_path, work_directory, dry_run=args.dry_run, auto_mode=args.auto, process_mode=args.mode, max_threads=args.threads)
