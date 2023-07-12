import os
import shutil


def cleanup_folder(path: str) -> None:
    if os.path.exists(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted file: {path}")
            elif os.path.isdir(file_path):
                # Delete the directory and its contents recursively
                shutil.rmtree(file_path)
                print(f"Deleted directory: {path}")
        print(f"Cleanup complete for folder: {path}")
    else:
        print(f"Folder not found: {path}")
