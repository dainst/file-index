import re
from datetime import date, datetime
import dateparser

import mimetypes
import os
import sys
import time
import logging

from lib import open_search

SIZE_PATTERN_PLAIN_BYTE_VALUE = r"^\d+$"
SIZE_PATTERN_VARIANT_1 = r"^.+\(([\d\.]+) Bytes\)$" # "481,6 KB (481.631 Bytes)"


HEADING_MAPPING = {
    "name": ["Name"],
    "path": ["Pfad"],
    "size_bytes": ["Größe"],
    "created": ["Erstelldatum"],
    "modified": ["Änderungsdatum"],
    "type": ["Art"],
    "volume": ["Name des Volumes"],
    "neofinder_catalog": ["Katalog"]
}

OPTIONAL_HEADINGS = ["media_info"]

DIRECTORY_VARIANTS = ["Ordner"]

DATE_FORMATS = ["%d.%m.%Y", "%d. %A %Y um %H:%M"]

overall_lines = 0
faulty_lines = 0
no_date = 0

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
            standardized.append(heading)

    if len(HEADING_MAPPING.keys() - standardized) != 0:
        logging.error(" The following column headings were expected but could not be mapped:")
        logging.error(HEADING_MAPPING.keys() - standardized)

        raise Exception("Required field not found.")

    return standardized

def parse_size_in_bytes(neofinder_value):

    if re.match(SIZE_PATTERN_PLAIN_BYTE_VALUE, neofinder_value):
        return int(neofinder_value)

    m = re.match(SIZE_PATTERN_VARIANT_1, neofinder_value)
    if m:
        return int(m.group(1).replace('.', ''))
        
    logging.warning(f" Unable to match neofinder size value {neofinder_value}.")

def process_values(values):
    global no_date

    # Remove all dictionary entries that are not defined as keys in the HEADING_MAPPING above
    values = dict(filter(lambda item: (item[0] in HEADING_MAPPING.keys()), values.items()))

    modified_standardized = dateparser.parse(values['modified'], date_formats=DATE_FORMATS)
    if not modified_standardized:
        if values['modified'] != "-":
            logging.info(f" Unable to parse modification date for '{values['path']}': '{values['modified']}'")
    
    created_standardized = dateparser.parse(values['created'], date_formats=DATE_FORMATS)
    if not created_standardized:
        # Old exports seem to have missing creation dates ('-' values), we fallback to the modified date.
        if values['created'] != "-":
            logging.info(f" Unable to parse creation date for '{values['path']}': '{values['created']}'")
        else:
            created_standardized = modified_standardized

    values['neofinder_created'] = values["created"]
    values['neofinder_modified'] = values["modified"]

    if not modified_standardized and not created_standardized:
        logging.info(f" Neither creation nor modification date found for '{values['path']}'.")
        no_date += 1

    values["created"] = created_standardized
    values["modified"] = modified_standardized

    values["neofinder_path"] = values["path"]
    values["path"] = values["path"].lstrip(f"{values['neofinder_catalog']}:") 
    values["path"] = values["path"].replace(":", "/")

    parsed_value = "unknown"
    if values["type"] != "":
        values["neofinder_type"] = values["type"]
        if values["type"].strip() != "-":
            if values["type"] in DIRECTORY_VARIANTS:
                parsed_value = "directory"
            else: 
                parsed_value = "file"
        
    values["type"] = parsed_value

    values["neofinder_size"] = values["size_bytes"]
    values["size_bytes"] = parse_size_in_bytes(values["size_bytes"])

    guessed_mimetype = mimetypes.guess_type(values["name"], strict=False)
    if guessed_mimetype[0]:
        values["mime_type"] = guessed_mimetype[0]

    return values


def process_file(path, index_name):
    global overall_lines
    global faulty_lines

    batch_size = 100000

    with open(path, 'r') as csv_file:
        line = csv_file.readline()
        headings = line.split('\t')

        headings = standardize_headings(headings)

        line_counter = 0
        batch = []
        found_first_data_row = False
        found_faulty_line = False
        next_line = csv_file.readline()
        while(next_line):
    
            values = next_line.split('\t')
            if len(values) == len(headings):
                if found_faulty_line:
                    logging.warning("Faulty line fixed.\n")
                    found_faulty_line = False

                found_first_data_row = True
                values[-1] = values[-1].strip() # remove newline character '\n'

                processed = process_values(dict(zip(headings, values)))
                # Using path as id caused issues because some path are longer than 512
                # thus too long for OpenSearch document ids.
                processed["_id"] = f"{os.path.basename(path)}-{line_counter}"
                batch.append(processed)

                line_counter += 1
                next_line = csv_file.readline()

            elif len(values) < len(headings) and found_first_data_row and next_line != "":

                sanitized = next_line.strip('\n')
                logging.warning("Possible faulty new line in data row:")
                logging.warning(f"'{sanitized}'")
                next_line = f"{sanitized}{csv_file.readline()}"
                
                found_faulty_line = True

                logging.warning("Recombined with following line to:")
                logging.warning(f"'{next_line.strip()}'")

                faulty_lines += 1

            elif len(values) > len(headings):
                logging.error("Failed to fix row, ended up with more columns than headings:")
                logging.error(f"'{next_line}'")

                next_line = csv_file.readline()

            else:
                next_line = csv_file.readline()

        
        if len(batch) > 0:
            open_search.push_batch(batch, index_name)
            logging.info(f" ...processed {line_counter}.")

        overall_lines += line_counter

if __name__ == '__main__':

    start_time = time.time()
    root_path = sys.argv[1].removesuffix("/")
    index_name = os.path.basename(root_path.lower())

    logging.basicConfig(
        filename=f'{index_name}_{date.today()}.log', 
        filemode='w',
        encoding='utf-8',
        format='%(asctime)s|%(levelname)s: %(message)s',
        level=logging.INFO
    )

    open_search.create_index(index_name)

    for f in os.scandir(root_path):
        if f.is_file() and f.name.endswith('.txt'):
            try:
                logging.info(f"Processing file '{f.name}'.")
                start_time_file = time.time()
                process_file(f.path, index_name)
                logging.info(f"Processed file in {round(time.time() - start_time_file, 2)} seconds.\n")
            except Exception as e:
                logging.error(f"Error when processing file '{f.name}'.")
                logging.error(e)
                logging.error("")

    logging.info(f"Finished after {round(time.time() - start_time, 2)} seconds.")
    logging.info(f"Indexed {overall_lines} rows.")
    logging.info(f"Indexed {no_date} rows without creation/modification date.")
