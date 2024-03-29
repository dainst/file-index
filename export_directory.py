from datetime import datetime, timezone, date
import os
import sys
import filetype
import mimetypes
import time
import logging
import argparse
import json
import hashlib

from lib import output_helper

batch = []
counter = 0

parser = argparse.ArgumentParser(description='Process file system tree.')
parser.add_argument('root_directory', type=str, help="The directory containing exported NeoFinder files (txt).")

# ptifs are not a default and otherwise will require filetype.guess which is much slower because it will
# open the file and decide by contents.
mimetypes.add_type("image/tiff", ".ptif")

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def walk_file_system(current, root_path, output_directory):
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

                document['modified'] = None
                document['created'] = None

                try:
                    document['modified'] = datetime.fromtimestamp(
                        stats.st_mtime, tz=timezone.utc)
                except:
                    logging.error(f"Unable to parse modified date {stats.st_mtime} for {f.path}.")

                try:
                    document['created'] = datetime.fromtimestamp(
                        stats.st_ctime, tz=timezone.utc)
                except:
                    logging.error(f"Unable to parse creation date {stats.st_ctime} for {f.path}.")

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
                        except OSError as e:
                            logging.error(f"Got exception for {f.path}.")
                            logging.error(e)
                            guess = None
                        if guess:
                            document["mime_type"] = guess.mime
                    
                    # with open(f.path, "rb") as f:
                    #     file_hash = hashlib.md5()

                    #     while chunk := f.read():
                    #         file_hash.update(chunk)

                    # document["md5"] = file_hash.hexdigest()

                document["_id"] = relative_path

                batch.append(document)
                counter += 1
            except FileNotFoundError:
                if f.is_symlink():
                    logging.warning(f"Found broken symlink: '{relative_path}'.")
                else:
                    logging.error(f"Unknown FileNotFoundError: '{relative_path}'.")

        if len(batch) > 100000:
            logging.info(f"...processed {counter}, exporting to file.")
            with open(f"{output_directory}/{counter}_files.json", 'w') as f:
                json.dump(batch, f, default=json_serial)
            
            batch = []

        for subdir in subdirs:
            walk_file_system(subdir, root_dir, output_directory)
    except PermissionError:
        logging.error(f"Got PermissionError for '{current}', ignoring.")
    except FileNotFoundError as e:
        logging.error(e)
        logging.error(f"Got a FileNotFoundError while processing the directory '{current}'. Maybe a directory or file got moved while processing?")

try:
    if __name__ == '__main__':

        start_time = time.time()

        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        options = vars(parser.parse_args())

        root_dir = options['root_directory'].removesuffix("/")
        input_dir_name = os.path.basename(root_dir).lower()

        logging.basicConfig(
            filename=f'{output_helper.get_logging_dir()}/directory_{input_dir_name}_{now}.log', 
            filemode='w',
            encoding='utf-8',
            format='%(asctime)s|%(levelname)s: %(message)s',
            level=logging.INFO
        )

        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

        output_directory = f"{output_helper.get_output_base_dir()}/directory_{input_dir_name}_{now}"
        try:
            os.mkdir(output_directory)
        except FileExistsError:
            logging.info(f"Output directory {output_directory} already exists.")

        logging.info(f"Scanning {root_dir}")
        walk_file_system(root_dir, root_dir, output_directory)

        if len(batch) > 0:
            with open(f"{output_directory}/{counter}_files.json", 'w') as f:
                json.dump(batch, f, default=json_serial)

        logging.info("####################################################################################")
        logging.info(f"Finished after {round(time.time() - start_time, 2)} seconds.")
        logging.info(f"Processed files {counter} overall.")
except Exception as e:
    logging.error("Encountered unhandled exception")
    logging.error(e)