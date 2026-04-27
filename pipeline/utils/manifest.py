import os
import json
import hashlib
import pandas as pd
from datetime import datetime


def get_checksum(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        while chunk := f.read(4096):
            sha256.update(chunk)

    return sha256.hexdigest()


def create_manifest(folder_path):
    manifest = []

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        if os.path.isfile(file_path) and file_name != "manifest.json":
            try:
                if file_name.endswith(".csv"):
                    df = pd.read_csv(file_path)

                elif file_name.endswith(".json"):
                    continue

                elif file_name.endswith(".parquet"):
                    df = pd.read_parquet(file_path)

                else:
                    continue

                file_info = {
                    "file_name": file_name,
                    "row_count": len(df),
                    "schema": {
                        col: str(dtype)
                        for col, dtype in df.dtypes.items()
                    },
                    "processing_timestamp": datetime.utcnow().isoformat() + "Z",
                    "sha256_checksum": get_checksum(file_path)
                }

                manifest.append(file_info)

            except Exception as e:
                print(f"Skipping {file_name}: {str(e)}")

    manifest_path = os.path.join(folder_path, "manifest.json")

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=4)

    print(f"Manifest created for {folder_path}")