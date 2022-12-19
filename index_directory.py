from datetime import datetime, timezone, date
import os
import requests
import sys
import filetype
import mimetypes
import time
import logging
import argparse
import json

from lib import open_search, output_helper

batch = []
counter = 0

parser = argparse.ArgumentParser(description='Process file system tree.')
parser.add_argument('root_directory', type=str, help="The directory containing exported NeoFinder files (txt). Its name will be used as the name for the target index.")
parser.add_argument('--clear', action='store_true',  dest='clear', help="Clear existing search index if found, default: false.")
parser.add_argument('--to-file', action='store_true', dest='to_file', help="Save result as JSON files, which can be indexed at a later point.")


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def walk_file_system(current, root_path, target_index, output_directory):
    global batch
    global counter

    subdirs = []
    try:
        for f in os.scandir(current):

            relative_path = f.path[len(root_path) + 1:]
            document = {
                'name': f.name,
                'path': relative_path
            }

            try:
                stats = f.stat()

                document['size_bytes'] = stats.st_size
                document['modified'] = datetime.fromtimestamp(
                    stats.st_mtime, tz=timezone.utc)
                document['created'] = datetime.fromtimestamp(
                    stats.st_ctime, tz=timezone.utc)

                if f.is_dir():
                    subdirs.append(f.path)
                    document['type'] = "directory"
                else:
                    document['type'] = "file"

                    guess = mimetypes.guess_type(f.name, strict=False)
                    if guess[0]:
                        document["mime_type"] = guess[0]
                    else:
                        try:
                            # fallback peeks into file and tries to decide by content
                            guess = filetype.guess(f.path)
                        except PermissionError:
                            guess = None
                        if guess:
                            document["mime_type"] = guess.mime

                document["_id"] = relative_path

                batch.append(document)
                counter += 1
            except FileNotFoundError:
                if f.is_symlink():
                    logging.warning(f"Found broken symlink: '{relative_path}'.")
                else:
                    logging.error(f"Unknown FileNotFoundError: '{relative_path}'.")

        if len(batch) > 100000:
            logging.info(f"...processed {counter}, pushing to index.")
            if output_directory:
                with open(f"{output_directory}/{counter}_files.json", 'w') as f:
                    json.dump(batch, f, default=json_serial)
            else:
                open_search.push_batch(batch, target_index, output_directory)
            batch = []

        for subdir in subdirs:
            walk_file_system(subdir, root_dir, target_index, output_directory)
    except PermissionError:
        logging.error(f"Got PermissionError for '{current}', ignoring.")


if __name__ == '__main__':

    start_time = time.time()

    options = vars(parser.parse_args())

    root_dir = options['root_directory'].removesuffix("/")
    target_index = os.path.basename(root_dir).lower()

    logging.basicConfig(
        filename=f'{output_helper.get_logging_dir()}/{target_index}_{date.today()}.log', 
        filemode='w',
        encoding='utf-8',
        format='%(asctime)s|%(levelname)s: %(message)s',
        level=logging.INFO
    )

    output_directory = None
    if options['to_file']:
        output_directory = f"{output_helper.get_output_base_dir()}/{target_index}_{date.today()}"
        try:
            os.mkdir(output_directory)
        except FileExistsError:
            logging.info(f"Output directory {output_directory} already exists.")
    else:
        open_search.create_index(target_index, options['clear'])

    logging.info(f"Scanning {root_dir}")
    walk_file_system(root_dir, root_dir, target_index, output_directory)

    if len(batch) > 0:
        if output_directory:
            with open(f"{output_directory}/{counter}_files.json", 'w') as f:
                json.dump(batch, f, default=json_serial)
        else:
            open_search.push_batch(batch, target_index, output_directory)
    logging.info(f"Processed {counter} overall.")

    logging.info(f"Finished after {round(time.time() - start_time, 2)} seconds.")