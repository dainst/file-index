from datetime import datetime, timezone
from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError
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
                        "type":"date",
                        "format": "strict_date_optional_time||epoch_millis"
                    },
                    "modified":{
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    }
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

    document = {
        'name': entry.name,
        'path': relative_path,
        'bytes': stats.st_size,
        'modified': datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc),
        'created': datetime.fromtimestamp(stats.st_ctime, tz=timezone.utc)
    }

    response = client.index(
        index = index_name,
        body = document,
        id = relative_path,
        refresh = True
    )

    print(f'Adding document: {relative_path}')