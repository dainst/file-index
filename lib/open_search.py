from datetime import datetime, timezone
from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import RequestError

import filetype
import os

password = os.environ.get('FILE_INDEX_PASSWORD')
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
                    "size": {
                        "type": "long"
                    },
                }
            }
        }

        response = client.indices.create(index_name, body=index_body)
        print(f'Created index index: {index_name}')
    except RequestError as e:
        client.indices.delete(index_name)
        create_index(index_name)

def push_to_index(entry: os.DirEntry, root_path: str, index_name: str):

    stats = entry.stat()

    relative_path = entry.path[len(root_path) + 1:]

    guess = filetype.guess(entry.path)
    
    if guess is not None:
        mime_type = guess.mime
    else:
        mime_type = None

    document = {
        'name': entry.name,
        'path': relative_path,
        'size_bytes': stats.st_size,
        'size': bytes_to_human_readable(stats.st_size),
        'mime_type': mime_type,
        'modified': datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc),
        'created': datetime.fromtimestamp(stats.st_ctime, tz=timezone.utc),
        'last_indexed': datetime.now()
    }

    response = client.index(
        index = index_name,
        body = document,
        id = relative_path,
        refresh = True
    )

    print(f'Adding document: {relative_path}')

def push_to_index_b(data, index_name):

    data["indexed"] = datetime.now()

    # TODO: Menschen lesbare size

    response = client.index(
        index = index_name,
        body = data,
        id = data["path"],
        refresh = True
    )

def push_batch(docs, index_name):

    data = []

    for doc in docs:
        doc["indexed"] = datetime.now()
        data.append({
            '_op_type': 'index',
            '_index': index_name,
            '_id': doc["path"],
            '_source': dict(sorted(doc.items()))
        })
    
    response = helpers.bulk(
        client,
        data
    )
def bytes_to_human_readable(number: int, suffix="B"):
    
    notations = ["B", "KB", "MB", "GB"]

    counter = 0
    while (counter < len(notations)):
        if number < 1000:
            return f"{number} {notations[counter]}" 
        
        number = round(number * 0.0009765625, 2)
        counter += 1

    return f"{number} {notations[:-1]}"
