from datetime import datetime, timezone
from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import RequestError

import os
import logging

host = None
port = None
user = None
password = None
use_ssl = False

with open('.env', 'r') as env_file:
    line = env_file.readline()
    while line:
        try:
            [key, val] = line.split('=')

            if key.strip()[0] == '#':
                # ignore lines that are commented out
                pass
            else:
                if key == "FILE_INDEX_HOST":
                    host = val.strip()
                if key == "FILE_INDEX_PORT":
                    port = val.strip()
                if key == "FILE_INDEX_USER":
                    user = val.strip()
                if key == "FILE_INDEX_PASSWORD":
                    password = val.strip()
                if key == "FILE_INDEX_USE_SSL":
                    if val.strip() == "True":
                        use_ssl = True
        except ValueError as e:
            # Ignore lines without a key value pair separted by '='
            pass
        line = env_file.readline()

if host is None:
    raise Exception("Found no FILE_INDEX_HOST in .env")
if port is None:
    raise Exception("Found no FILE_INDEX_PORT in .env")
if user is None:
    raise Exception("Found no FILE_INDEX_USER in .env")
if password is None:
    raise Exception("Found no FILE_INDEX_PASSWORD in .env")

auth = (user, password)

client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    use_ssl = use_ssl
)

logging.getLogger('opensearch').setLevel(logging.WARNING)

def create_index(index_name, clear=False):
    try:
        # Create an index with non-default settings.
        index_body = {
            "mappings":{
                "properties":{
                    "created":{
                        "type":"date"
                    },
                    "modified":{
                        "type": "date"
                    },
                    "indexed": {
                        "type": "date"
                    },
                    "size_bytes": {
                        "type": "long"
                    },
                }
            }
        }

        response = client.indices.create(index_name, body=index_body)
        logging.info(f"Created index: '{index_name}'.\n")
    except RequestError as e:
        if e.status_code == 400 and e.error == "resource_already_exists_exception":
            if clear:
                logging.info(f"'{index_name}' index already exists, recreating...")
                client.indices.delete(index_name)
                logging.info(f"Deleted index: '{index_name}'.")
                create_index(index_name)
            else:
                logging.info(f"'{index_name}' index found...")
        else:
            raise e

def push_batch(docs, index_name):

    data = []

    for doc in docs:

        doc["indexed"] = datetime.now()
        doc["size"] = bytes_to_human_readable(doc["size_bytes"])
        _id = doc["_id"]
        del doc["_id"]

        data.append({
            '_op_type': 'index',
            '_index': index_name,
            '_id': _id,
            '_source': dict(sorted(doc.items()))
        })
    
    (successes, errors) = helpers.bulk(
        client,
        data
    )

    if successes != len(docs):
        logging.error(f"Got {len(errors)} while bulk indexing {len(docs)} documents:")
        logging.error(errors)

def bytes_to_human_readable(number: int):
    if number is None:
        return f"Unknown"
    
    notations = ["B", "KB", "MB", "GB"]

    counter = 0
    while (counter < len(notations)):
        if number < 1000:
            return f"{number} {notations[counter]}" 
        
        number = round(number * 0.001, 2)
        counter += 1

    return f"{number} {notations[-1]}"
