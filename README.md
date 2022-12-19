# Prerequisites

* Python > 3.8
* Docker and docker-compose (if you want to run OpenSearch Dashboards locally)

# Setup

Install python dependencies

```
pip3 install -r requirements.txt
```

Create a new `.env` file based on the template

```
cp .env_template .env
```

Adjust the `.env` file for your setup. The scripts read the connection info for the search index from the file.

# Import Script Usage

```
python3 index_directory.py <path to root directory>
```

The above command will (re-)create an index with the name of the root directory, process all files and subdirectories and push their data to the created index.

```
python3 index_neofinder.py <path to directory containing neofinder exports>
```

The above command will (re-)create an index with the name of the provided directory, process all `txt` files it finds within (ignoring subdirectories) and push their data to the created index.

```
python index_preprocessed.py <path to directory containing preprocessed files>
```

The above script will index preprocessed data created by the other two python scripts if they have been run using the `--to-file` option.

# Running local OpenSearch

```
docker-compose up
```

This will run OpenSearch on port 9200 and OpenSearch Dashboards on port 5601 locally.