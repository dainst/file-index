from datetime import datetime, timezone
from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import RequestError

import os
import logging

password = os.environ.get('FILE_INDEX_PASSWORD')

if password is None:
    with open('.env', 'r') as env_file:
        line = env_file.readline()
        while line:
            [key, val] = line.split('=')
            if key == "FILE_INDEX_PASSWORD":
                password = val.strip()
                break

            line = env_file.readline()

    if password is None:
        raise Exception("FILE_INDEX_PASSWORD environment variable not found!")

host = 'localhost'
port = 9200
auth = ('admin', password)

client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    use_ssl = False
)

logging.getLogger('opensearch').setLevel(logging.WARNING)

def create_index(index_name):
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
            logging.info(f"'{index_name}' index already exists, recreating...")
            client.indices.delete(index_name)
            logging.info(f"Deleted index: '{index_name}'.")
            create_index(index_name)
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
        raise Exception(errors)

def bytes_to_human_readable(number: int):
    
    notations = ["B", "KB", "MB", "GB"]

    counter = 0
    while (counter < len(notations)):
        if number < 1000:
            return f"{number} {notations[counter]}" 
        
        number = round(number * 0.001, 2)
        counter += 1

    return f"{number} {notations[-1]}"
