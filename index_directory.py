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

                batch.append(document)
                counter += 1
            except FileNotFoundError:
                if f.is_symlink():
                    print(f" found broken symlink: '{relative_path}'.")
                else:
                    print(f" unknown FileNotFoundError: '{relative_path}'.")

        if len(batch) > 100000:
            print(f" processed {counter}, pushing to index...")
            open_search.push_batch(batch, target_index)
            batch = []

        for subdir in subdirs:
            walk_file_system(subdir, root_dir, target_index)
    except PermissionError:
        print(f" got PermissionError for '{current}', ignoring.")


if __name__ == '__main__':
    root_dir = sys.argv[1].removesuffix("/")
    target_index = os.path.basename(root_dir).lower()

    open_search.create_index(target_index)

    walk_file_system(root_dir, root_dir, target_index)

    if len(batch) > 0:
        open_search.push_batch(batch, target_index)
        print(f" processed {counter} overall")