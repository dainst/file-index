import os
import requests
import sys

import lib.open_search as open_search

def walk_file_system(current, root_path, target_index):

    for f in os.scandir(current):
        if f.is_dir():
            walk_file_system(f.path, root_path, target_index)
        else:
            open_search.push_to_index(f, root_path, target_index)

root_dir = sys.argv[1].removesuffix("/")
target_index = os.path.basename(root_dir).lower()

open_search.create_index(target_index)

walk_file_system(root_dir, root_dir, target_index)