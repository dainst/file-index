import re
from datetime import datetime
import dateparser

import mimetypes
import os

from lib import open_search

CATALOG_REGEX = r"Name:? (.+)"

HEADING_MAPPING = {
    "name": ["Name"],
    "path": ["Pfad"],
    "size": ["Größe"],
    "created": ["Erstelldatum"],
    "modified": ["Änderungsdatum"],
    "type": ["Art"],
    "volume": ["Name des Volumes"],
    "neofinder_catalog": ["Katalog"]
    # "media_info": ["Media-Info"]
}

OPTIONAL_HEADINGS = ["media_info"]

DIRECTORY_VARIANTS = ["Ordner"]

DATE_FORMATS = ["%d.%m.%Y", "%d. %A %Y um %H:%M"]

def standardize_headings(headings):

    standardized = []

    for heading in headings:

        heading = heading.strip()
        updated = None

        for (standard, variants) in HEADING_MAPPING.items():
            if heading in variants:
                updated = standard
        
        if updated:
            standardized.append(updated)
        else:
            # print(f"Found no standardized heading value für {heading}")
            standardized.append(heading)

    if len(HEADING_MAPPING.keys() - standardized) != 0:
        print("The following column headings were expected but could not be mapped:")
        print(HEADING_MAPPING.keys() - standardized)

        raise Exception("Required field not found.")

    return standardized


def process_values(values):

    # Remove all dictionary entries that are not defined as keys in the HEADING_MAPPING above
    values = dict(filter(lambda item: (item[0] in HEADING_MAPPING.keys()), values.items()))

    modified_standardized = dateparser.parse(values['modified'], date_formats=DATE_FORMATS)
    if not modified_standardized:
        print(f"Unable to parse modification date for {values['path']}: '{values['modified']}'")
    
    created_standardized = dateparser.parse(values['created'], date_formats=DATE_FORMATS)
    if not created_standardized:
        # Old exports seem to have missing creation dates ('-' values), we fallback to the modified date.
        if values['created'] != "-":
            print(f"Unable to parse creation date for {values['path']}: '{values['created']}'")
        else:
            created_standardized = modified_standardized

    values['neofinder_created'] = values["created"]
    values['neofinder_modified'] = values["modified"]

    values["created"] = created_standardized
    values["modified"] = modified_standardized

    values["neofinder_path"] = values["path"]
    values["path"] = values["path"].replace(":", "/") 

    values["neofinder_type"] = values["type"]
    if values["type"] in DIRECTORY_VARIANTS:
        values["type"] = "directory"
    else:
        values["type"] = "file"

    guessed_mimetype = mimetypes.guess_type(values["name"], strict=False)
    if guessed_mimetype[0]:
        values["mime_type"] = guessed_mimetype[0]

    return values


def process_file(path, index_name):
    with open(path, 'r') as csv_file:
        line = csv_file.readline()
        headings = line.split('\t')

        headings = standardize_headings(headings)

        line_counter = 0
        while(line):
            line = csv_file.readline()
    
            values = line.split('\t')
            if len(values) == len(headings):
                processed = process_values(dict(zip(headings, values)))

                open_search.push_to_index_b(processed, index_name)
                line_counter += 1

                if line_counter % 1000 == 0:
                    print(f"  processed {line_counter}")

index_name = "neo_finder"

open_search.create_index(index_name)

for f in os.scandir("neofinder_data"):
    if f.is_file() and f.name.endswith('.txt'):
        try:
            print(f"Processing file {f.name}")
            process_file(f.path, index_name)
        except Exception as e:
            print(f"Error when processing file {f.name}.")
            print(e)
