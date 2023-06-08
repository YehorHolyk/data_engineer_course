from typing import List, Dict, Any
import json
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


def save_to_disk(json_content: List[Dict[str, Any]],
                 folder_path: str,
                 file_name: str) -> None:
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    else:
        cleanup_folder(folder_path)

    path_to_save = os.path.join(folder_path, file_name)

    with open(path_to_save, 'w') as file:
        json_content = json.dumps(json_content)
        file.write(json_content)
