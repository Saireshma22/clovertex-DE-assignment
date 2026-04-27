import json
from datetime import datetime


def log_dataset(
    dataset_name,
    rows_in,
    rows_out,
    duplicates_removed,
    nulls_handled,
    encoding_fixed
):
    log = {
        "dataset": dataset_name,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "issues_found": {
            "duplicates_removed": duplicates_removed,
            "nulls_handled": nulls_handled,
            "encoding_fixed": encoding_fixed
        },
        "processing_timestamp": datetime.utcnow().isoformat() + "Z"
    }

    print(json.dumps(log, indent=4))