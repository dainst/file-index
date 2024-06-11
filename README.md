This repository contains three scripts for indexing file information in [OpenSearch](https://opensearch.org/).

# Prerequisites

* Python > 3.8
* Docker and docker-compose (if you want to run OpenSearch Dashboards locally)

## Setup

Install python dependencies

```bash
pip3 install -r requirements_export.txt # required for export_directory.py and export_neofinder.py 
pip3 install -r requirements_import.txt # required for import.py
```

```
cp .env_template .env
```

Adjust the `.env` file for your setup, the scripts read the connection info for the OpenSearch from this file.

# Usage

There are three main scripts
* `export_directory.py`
* `export_neofinder.py`
* `import.py`

The first two create JSON objects that can be imported using the third script.

## Processing file system data

In order to recursively scan a directory and transform file and directory information into JSON run:

```
python3 export_directory.py <path to your directory>
```

## Processing NeoFinder exports

In order to transform NeoFinder exports (txt files that are basically csv) into JSON run:

```
python3 export_neofinder.py <path to directory containing neofinder export txts>
```

## Importing into OpenSearch

Both scripts above will produce the following results:
* a log of the export in [log](log).
* a directory of JSON files in [output](output).

The `import.py` will read the URL and credentials for your OpenSearch installation from the `.env` file you created in the setup above.

```
python3 import.py <opensearch index for your data> <path to your JSON directory> 
```

By default, the script will add or update data if the specified index already exists in OpenSearch. If the index does not exist, the script will create a new one.

If you want to delete the existing index before importing, you may run the script with the `--clear` option.

```
python3 import.py <opensearch index for your data> <path to your JSON directory> --clear
```

# Running OpenSearch

## Locally

If you want to run a local installation of OpenSearch, you can start one with.

```
docker-compose up
```

This will run OpenSearch on port 9200 and OpenSearch Dashboards on port 5601 locally.

The default credentials are admin:admin, which should also be reflected in your `.env` file before running `import.py`.

## Deployment
__You should not run OpenSearch/OpenSearch Dashboards with the default credentials.__ See the official [documentation](https://opensearch.org/docs/2.4/security/authentication-backends/authc-index/). 
Set your `.env` file according to your domain and updated credentials. 

Start OpenSearch, OpenSearchDashboards and Traefik with:
```
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

