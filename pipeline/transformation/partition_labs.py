import os


def partition_lab_results(labs):
    output_path = (
        "datalake/refined/lab_results"
    )

    os.makedirs(
        output_path,
        exist_ok=True
    )

    labs.to_parquet(
        output_path,
        partition_cols=["test_name"],
        index=False
    )

    print("Lab results partitioned successfully")