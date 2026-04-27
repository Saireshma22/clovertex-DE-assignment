import os
import shutil

def copy_raw_files():
    source_folder = "data"
    target_folder = "datalake/raw"

    for file_name in os.listdir(source_folder):
        source_path = os.path.join(source_folder, file_name)

        if os.path.isfile(source_path):
            target_path = os.path.join(target_folder, file_name)
            shutil.copy(source_path, target_path)

    print("Raw files copied successfully")