import pandas as pd

def generate_patient_summary(patients):
    summary = {
        "total_patients": [len(patients)],
        "male_count": [
            len(patients[patients["sex"] == "Male"])
        ],
        "female_count": [
            len(patients[patients["sex"] == "Female"])
        ],
        "site_count": [
            patients["site"].nunique()
        ]
    }

    df = pd.DataFrame(summary)

    df.to_parquet(
        "datalake/consumption/patient_summary.parquet",
        index=False
    )

    print("patient_summary.parquet created")


def generate_lab_statistics(labs):
    stats = labs.groupby("test_name")["test_value"].agg(
        mean="mean",
        median="median",
        std="std"
    ).reset_index()

    # Simple abnormal flag
    stats["abnormal_flag"] = stats["mean"] > 100

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
    filtered = genomics[
        genomics["clinical_significance"].isin(
            ["Pathogenic", "Likely Pathogenic"]
        )
    ]

    hotspots = filtered.groupby("gene").agg(
        variant_count=("gene", "count"),
        mean_af=("allele_frequency", "mean"),
        percentile_25=("allele_frequency",
                        lambda x: x.quantile(0.25)),
        percentile_75=("allele_frequency",
                        lambda x: x.quantile(0.75))
    ).reset_index()

    hotspots = hotspots.sort_values(
        by="variant_count",
        ascending=False
    ).head(5)

    hotspots.to_parquet(
        "datalake/consumption/variant_hotspots.parquet",
        index=False
    )

    print("variant_hotspots.parquet created")


def generate_anomaly_flags(labs, genomics):
    # Lab anomalies
    lab_anomalies = labs[
        (labs["test_value"] < 0) |
        (labs["test_value"] > 1000)
    ]

    # Genomics anomalies
    genomics_anomalies = genomics[
        (genomics["allele_frequency"] > 1) |
        (genomics["allele_frequency"] < 0) |
        (genomics["read_depth"] < 0)
    ]

    # Standardize columns
    lab_anomalies = lab_anomalies.copy()
    lab_anomalies["anomaly_type"] = "lab_value_issue"

    genomics_anomalies = genomics_anomalies.copy()
    genomics_anomalies["anomaly_type"] = "genomics_issue"

    # Combine anomalies
    anomalies = pd.concat(
        [lab_anomalies, genomics_anomalies],
        ignore_index=True,
        sort=False
    )

    anomalies.to_parquet(
        "datalake/consumption/anomaly_flags.parquet",
        index=False
    )

    print(f"Anomalies detected: {len(anomalies)}")

    print("anomaly_flags.parquet created")


def generate_high_risk_patients(labs, genomics):
    # HbA1c high-risk patients
    diabetic = labs[
        (labs["test_name"].str.lower() == "hba1c") &
        (labs["test_value"] > 7)
    ]

    diabetic_ids = set(diabetic["patient_ref"])

    # Pathogenic genomics patients
    pathogenic = genomics[
        genomics["clinical_significance"].isin(
            ["Pathogenic", "Likely Pathogenic"]
        )
    ]

    pathogenic_ids = set(pathogenic["patient_ref"])

    # Cross-domain intersection
    high_risk_ids = diabetic_ids.intersection(
        pathogenic_ids
    )

    high_risk = pd.DataFrame({
        "patient_ref": list(high_risk_ids)
    })

    high_risk.to_parquet(
        "datalake/consumption/high_risk_patients.parquet",
        index=False
    )

    print("high_risk_patients.parquet created")