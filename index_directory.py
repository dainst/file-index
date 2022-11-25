from datetime import datetime, timezone
import os
import requests
import sys
import filetype
import mimetypes
import lib.open_search as open_search

batch = []
counter = 0


def walk_file_system(current, root_path, target_index):
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
                document['modified'] = datetime.fromtimestamp(
                    stats.st_mtime, tz=timezone.utc)
                document['created'] = datetime.fromtimestamp(
                    stats.st_ctime, tz=timezone.utc)

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
                        if guess:
                            document["mime_type"] = guess.mime

                document["_id"] = relative_path

                batch.append(document)
                counter += 1
            except FileNotFoundError:
                if f.is_symlink():
                    logging.warning(f"Found broken symlink: '{relative_path}'.")
                else:
                    logging.error(f"Unknown FileNotFoundError: '{relative_path}'.")

        if len(batch) > 100000:
            logging.info(f"...processed {counter}, pushing to index.")
            open_search.push_batch(batch, target_index)
            batch = []

        for subdir in subdirs:
            walk_file_system(subdir, root_dir, target_index)
    except PermissionError:
        logging.error(f"Got PermissionError for '{current}', ignoring.")


if __name__ == '__main__':
    start_time = time.time()

    root_dir = sys.argv[1].removesuffix("/")
    target_index = os.path.basename(root_dir).lower()

    open_search.create_index(target_index)


    logging.basicConfig(
        filename=f'{target_index}_{date.today()}.log', 
        filemode='w',
        encoding='utf-8',
        format='%(asctime)s|%(levelname)s: %(message)s',
        level=logging.INFO
    )

    walk_file_system(root_dir, root_dir, target_index)

    if len(batch) > 0:
        open_search.push_batch(batch, target_index)
        logging.info(f"Processed {counter} overall.")

    logging.info(f"Finished after {round(time.time() - start_time, 2)} seconds.")