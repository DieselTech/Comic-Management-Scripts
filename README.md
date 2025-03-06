# Comic-Mangement-Scripts


# Library Management 

create_fake_comic_library.py - This will create folders and cbz files with some random names to simulate a comic book library. Inserts a single image (that you define) so that it's a valid cbz file. 

find_junk_in_cbz.py - Scans a path the user supplies for cbz files and records how many "junk" files are inside. Junk files are considered anything not an image or .xml metadata. Stores results in non_image_files.log

mimic_files_and_folder_strcuture.py - Will look at a folder on disk and mimic its structure of folders and files into a fake library of 0 byte files. This lets you test regex patterns against "real" files and folders easier without always having to copy a working data set each time you want to run a script against it. 

record_comicinfo_to_sqlite.py - Reads your cbz collection and records the contents of `comicinfo.xml` into a sqlite database. This is useful for quickly searching against your collection of metadata to see what might need to be fixed or updated. Records last time of scan and only scans files that have changed in order to be more efficient. 

Manga_Download_Processor.py - Takes a folder you have with raw download files and tries to determine the series name, volume/chapter number and then converts the images to webp format. Moves the converted folder to your library folder with the cleaned up file naming. 

- Todo:
  - Instead of just moving the file to the final library based on the series name, enumerate the folders in the final library and pick the one that seems like it is the closest match. That way small differences in naming don't interfear with movin the final file.
  - Test more regex patterns to cover different naming conventions. I have a good number of the most common use cases already figured out.
  - Change the way the (F) system works. Fixed files could have numbers in them like `(F3)` in cases where there might of been a lot of re-releases. Need to account for (F#) 



# Kavita Specific files 

scan_all_endpoints_API.py - For kavita to start a scan on every library in your server. 

scan_all_libraries.py - For Kavita, but was made before the "scan-all" endpoint was made. This gets the list of your libraries on the server and sends the scan command to them 1 by 1. 

kavita_create_library_per_folder.py - This will look at a root folder and add all sub-folders to your Kavita instance as their own library. Is able to work across the network and different OS's to send to kavita's API.

kavita_delete_all_libraries.py - Tactial nuke for Kavita's libraries. Deletes every library listed without any filtering options. 

kavita_to_kitty.py (non-working) - Eventually will scan your kavita manga library to find series, then match those series off a certain kitty site to find new posts.
