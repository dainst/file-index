
from datetime import date, datetime
import argparse
import json
import os
import logging
from lib import open_search, output_helper
import time

parser = argparse.ArgumentParser(description='Index result files preprocessed by "index_neofinder.py" or "index_directory.py".')
parser.add_argument('index_name', type=str, help="The index the data should be imported to.")
parser.add_argument('root_directory', type=str, help="The directory containing preprocessed files (json).")
parser.add_argument('--clear', action='store_true',  dest='clear', help="Clear existing index if found, optional.")


options = vars(parser.parse_args())

if __name__ == '__main__':

    start_time = time.time()
    index_name = options["index_name"]
    root_path = options["root_directory"].removesuffix("/")

    logging.basicConfig(
	filename=f'{output_helper.get_logging_dir()}/{index_name}_import_{date.today()}.log'
        filemode='w',
        encoding='utf-8',
        format='%(asctime)s|%(levelname)s: %(message)s',
        level=logging.INFO
    )

    open_search.create_index(index_name, options['clear'])

    for f in os.scandir(root_path):
        if f.is_file() and f.name.endswith('.json'):
            with open(f, 'r') as file_handle:
                logging.info(f"Processing file '{f.name}'.")
                open_search.push_batch(json.loads(file_handle.read()), index_name)

    logging.info(f"Finished after {round(time.time() - start_time, 2)} seconds.")
