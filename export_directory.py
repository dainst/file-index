import argparse
import json
import logging
import mimetypes
import os
import sys
import time
from datetime import date, datetime, timezone

import filetype

from lib import output_helper

batch = []
counter = 0

zero_byte_file_paths = []

tivoli_output_flag = False

parser = argparse.ArgumentParser(description="Walks a file system tree and generates standardized JSON metadata to be indexed in OpenSearch.")
parser.add_argument(
    "root_directory",
    type=str,
    help="The directory containing exported NeoFinder files (txt).",
)
parser.add_argument("--tivoli", dest="tivoli", action="store_true", help="If set the script will add additional fields that are normally only part of the output `export_neofinder.py` produces. It will also use different names for the output directories and log files.")

mimetypes.add_type("image/tiff", ".ptif")


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def walk_file_system(current, root_path, output_directory):
    global batch
    global counter
    global zero_byte_file_paths
    global tivoli_output_flag

    subdirs = []
    try:
        for f in os.scandir(current):
            relative_path = f.path[len(root_path) + 1 :]
            document = {
                "name": f.name,
                "path": relative_path,
                "size_bytes": None,
                "modified": None,
                "created": None,
            }

            if tivoli_output_flag:
                base_directory = os.path.basename(root_path)

                document["neofinder_catalog"] = base_directory
                document["neofinder_volume"] = base_directory

            try:
                stats = f.stat()

                document["size_bytes"] = stats.st_size

                try:
                    document["modified"] = datetime.fromtimestamp(
                        stats.st_mtime, tz=timezone.utc
                    )
                except Exception:
                    logging.error(
                        f"Unable to parse modified date {stats.st_mtime} for {f.path}."
                    )

                try:
                    document["created"] = datetime.fromtimestamp(
                        stats.st_ctime, tz=timezone.utc
                    )
                except Exception:
                    logging.error(
                        f"Unable to parse creation date {stats.st_ctime} for {f.path}."
                    )

                document["_id"] = relative_path

                if f.is_dir():
                    subdirs.append(f.path)
                    document["type"] = "directory"
                else:
                    document["type"] = "file"

                    guess = mimetypes.guess_type(f.name, strict=False)
                    if guess[0]:
                        document["mime_type"] = guess[0]
                    else:
                        try:
                            guess = filetype.guess(f.path)
                        except PermissionError:
                            guess = None
                        except OSError as e:
                            logging.error(f"Got exception for {f.path}.")
                            logging.error(e)
                            guess = None
                        if guess:
                            document["mime_type"] = guess.mime

                    if stats.st_size == 0:
                        zero_byte_file_paths.append(relative_path)

                batch.append(document)
                counter += 1

            except FileNotFoundError:
                if f.is_symlink():
                    logging.warning(f"Found broken symlink: '{relative_path}'.")
                else:
                    logging.error(f"Unknown FileNotFoundError: '{relative_path}'.")

        if len(batch) > 100000:
            logging.info(f"...processed {counter}, exporting to file.")
            with open(f"{output_directory}/{counter}_files.json", "w") as f:
                json.dump(batch, f, default=json_serial)

            batch = []

        for subdir in subdirs:
            walk_file_system(subdir, root_dir, output_directory)

    except PermissionError:
        logging.error(f"Got PermissionError for '{current}', ignoring.")
    except FileNotFoundError as e:
        logging.error(e)
        logging.error(
            f"Got a FileNotFoundError while processing the directory '{current}'."
        )


try:
    if __name__ == "__main__":
        start_time = time.time()

        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        options = vars(parser.parse_args())

        root_dir = options["root_directory"].removesuffix("/")
        tivoli_output_flag = options["tivoli"]
        input_dir_name = os.path.basename(root_dir).lower()

        if tivoli_output_flag:
            log_output = f"{output_helper.get_logging_dir(input_dir_name)}/tivoli_{input_dir_name}_{now}.log"
        else:
            log_output = f"{output_helper.get_logging_dir(input_dir_name)}/directory_{input_dir_name}_{now}.log"

        logging.basicConfig(
            filename=log_output,
            filemode="w",
            encoding="utf-8",
            format="%(asctime)s|%(levelname)s: %(message)s",
            level=logging.INFO,
        )

        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

        if tivoli_output_flag:
            output_directory = f"{output_helper.get_output_base_dir(input_dir_name)}/tivoli_{input_dir_name}_{now}"
        else:
            output_directory = f"{output_helper.get_output_base_dir(input_dir_name)}/directory_{input_dir_name}_{now}"

        try:
            os.makedirs(output_directory)
        except FileExistsError:
            logging.info(f"Output directory {output_directory} already exists.")

        logging.info(f"Scanning {root_dir}")
        walk_file_system(root_dir, root_dir, output_directory)

        if len(batch) > 0:
            with open(f"{output_directory}/{counter}_files.json", "w") as f:
                json.dump(batch, f, default=json_serial)

            with open(f"{output_directory}/empty_files.txt", "w") as f:
                data = "\n".join(zero_byte_file_paths)
                f.write(data)

        logging.info(f"Finished after {round(time.time() - start_time, 2)} seconds.")
        logging.info(f"Processed files {counter} overall.")
        logging.info(f"Found {len(zero_byte_file_paths)} empty files.")

except Exception as e:
    logging.error("Encountered unhandled exception")
    logging.error(e)
