import os
import zipfile
import sqlite3
import glob
import xml.etree.ElementTree as ET
from tqdm import tqdm
from io import BytesIO

# Function to extract information from comicinfo.xml file
def extract_comic_info_from_zip(zip_ref, zip_info):
    with zip_ref.open(zip_info) as xml_file:
        xml_content = xml_file.read()
        tree = ET.fromstring(xml_content)
        root = tree

        # Extracting values from XML
        title = root.findtext('Title', default='')
        series = root.findtext('Series', default='')
        number = root.findtext('Number', default='')
        volume = root.findtext('Volume', default='')
        summary = root.findtext('Summary', default='')
        writer = root.findtext('Writer', default='')
        penciller = root.findtext('Penciller', default='')
        inker = root.findtext('Inker', default='')
        colorist = root.findtext('Colorist', default='')
        letterer = root.findtext('Letterer', default='')
        cover_artist = root.findtext('CoverArtist', default='')
        editor = root.findtext('Editor', default='')
        publisher = root.findtext('Publisher', default='')
        imprint = root.findtext('Imprint', default='')
        web = root.findtext('Web', default='')
        genre = root.findtext('Genre', default='')
        page_count = root.findtext('PageCount', default='')
        language_iso = root.findtext('LanguageISO', default='')
        comic_format = root.findtext('Format', default='')
        age_rating = root.findtext('AgeRating', default='')

        # Returning a dictionary containing extracted information
        return {
            'Title': title,
            'Series': series,
            'Number': number,
            'Volume': volume,
            'Summary': summary,
            'Writer': writer,
            'Penciller': penciller,
            'Inker': inker,
            'Colorist': colorist,
            'Letterer': letterer,
            'CoverArtist': cover_artist,
            'Editor': editor,
            'Publisher': publisher,
            'Imprint': imprint,
            'Web': web,
            'Genre': genre,
            'PageCount': page_count,
            'LanguageISO': language_iso,
            'Format': comic_format,
            'AgeRating': age_rating
        }

# Function to process zip files and record information in SQLite database
def process_zip_files(directory, database):
    conn = sqlite3.connect(database)
    c = conn.cursor()

    # Creating the table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS comics
                 (id INTEGER PRIMARY KEY, filename TEXT, path TEXT, title TEXT, series TEXT, number TEXT, volume TEXT, summary TEXT, 
                 writer TEXT, penciller TEXT, inker TEXT, colorist TEXT, letterer TEXT, cover_artist TEXT, editor TEXT, 
                 publisher TEXT, imprint TEXT, web TEXT, genre TEXT, page_count TEXT, language_iso TEXT, format TEXT, age_rating TEXT, last_modified INTEGER)''')
    
    conn.execute("PRAGMA journal_mode = WAL;")  # Enable WAL mode


    zip_files = glob.glob(os.path.join(directory, '**/*.cbz'), recursive=True)

    # Iterating through cbz files
    for zip_file in tqdm(zip_files, desc="Processing ZIP files", unit="file"):
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                last_modified = os.path.getmtime(zip_file)
                c.execute("SELECT last_modified FROM comics WHERE filename=?", (os.path.basename(zip_file),))
                result = c.fetchone()
                if result and result[0] >= last_modified:
                    continue  # Skip if the file hasn't been modified
                for zip_info in zip_ref.infolist():
                    if zip_info.filename.lower() == 'comicinfo.xml':
                        comic_info = extract_comic_info_from_zip(zip_ref, zip_info.filename)
                        c.execute('''INSERT INTO comics (filename, path, title, series, number, volume, summary, writer, penciller, inker,
                                     colorist, letterer, cover_artist, editor, publisher, imprint, web, genre, page_count,
                                     language_iso, format, age_rating, last_modified) 
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                  (os.path.basename(zip_file), os.path.dirname(zip_file), comic_info['Title'], comic_info['Series'], comic_info['Number'],
                                   comic_info['Volume'], comic_info['Summary'], comic_info['Writer'], comic_info['Penciller'],
                                   comic_info['Inker'], comic_info['Colorist'], comic_info['Letterer'],
                                   comic_info['CoverArtist'], comic_info['Editor'], comic_info['Publisher'],
                                   comic_info['Imprint'], comic_info['Web'], comic_info['Genre'], comic_info['PageCount'],
                                   comic_info['LanguageISO'], comic_info['Format'], comic_info['AgeRating'], last_modified))
                        conn.commit()
        except Exception as e:
            print(f"Error processing {zip_file}: {e}")

    conn.close()

# Example usage
directory = '/Comics'
database = 'comics_database.db'
process_zip_files(directory, database)
