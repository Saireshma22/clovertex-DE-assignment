import pandas as pd


def generate_patient_summary(patients):
    summary = {
        "total_patients": [len(patients)],
        "male_count": [len(patients[patients["sex"] == "Male"])],
        "female_count": [len(patients[patients["sex"] == "Female"])]
    }

    df = pd.DataFrame(summary)

    df.to_parquet(
        "datalake/consumption/patient_summary.parquet",
        index=False
    )

    print("patient_summary.parquet created")


def generate_lab_statistics(labs):
    stats = labs.groupby("test_name")["test_value"].agg(
        ["mean", "median", "std"]
    ).reset_index()

    stats.to_parquet(
        "datalake/consumption/lab_statistics.parquet",
        index=False
    )

    print("lab_statistics.parquet created")


def generate_diagnosis_frequency(diagnosis):
    freq = diagnosis["icd10_code"].value_counts().reset_index()

    freq.columns = ["icd10_code", "count"]

    freq.to_parquet(
        "datalake/consumption/diagnosis_frequency.parquet",
        index=False
    )

    print("diagnosis_frequency.parquet created")


def generate_variant_hotspots(genomics):
    hotspots = genomics["gene"].value_counts().head(5).reset_index()

    hotspots.columns = ["gene", "count"]

    hotspots.to_parquet(
        "datalake/consumption/variant_hotspots.parquet",
        index=False
    )

    print("variant_hotspots.parquet created")


def generate_anomaly_flags(patients):
    anomalies = patients[
        patients["date_of_birth"].isnull()
    ]

    anomalies.to_parquet(
        "datalake/consumption/anomaly_flags.parquet",
        index=False
    )

    print("anomaly_flags.parquet created")


def generate_high_risk_patients(labs, genomics):
    hba1c = labs[
        (labs["test_name"].str.lower() == "hba1c") &
        (labs["test_value"] > 7)
    ]

    risky_patients = hba1c[
        ["patient_ref"]
    ].drop_duplicates()

    risky_patients.to_parquet(
        "datalake/consumption/high_risk_patients.parquet",
        index=False
    )

    print("high_risk_patients.parquet created")