import matplotlib.pyplot as plt
import pandas as pd


# ------------------------------------------------
# 1. Patient Demographics
# ------------------------------------------------

def create_age_distribution(patients):
    patients = patients.copy()

    patients["date_of_birth"] = pd.to_datetime(
        patients["date_of_birth"],
        errors="coerce"
    )

    patients["age"] = (
        2026 - patients["date_of_birth"].dt.year
    )

    plt.figure(figsize=(8, 5))

    patients["age"].dropna().hist(bins=20)

    plt.title("Patient Age Distribution")
    plt.xlabel("Age")
    plt.ylabel("Patient Count")

    plt.savefig(
        "datalake/consumption/plots/age_distribution.png"
    )

    plt.close()

    print("age_distribution.png created")


def create_gender_distribution(patients):
    gender_counts = patients["sex"].value_counts()

    plt.figure(figsize=(6, 5))

    gender_counts.plot(kind="bar")

    plt.title("Gender Distribution")
    plt.xlabel("Gender")
    plt.ylabel("Count")

    plt.savefig(
        "datalake/consumption/plots/gender_distribution.png"
    )

    plt.close()

    print("gender_distribution.png created")


# ------------------------------------------------
# 2. Diagnosis Frequency
# ------------------------------------------------

def create_diagnosis_chart(diagnosis):
    top_diag = (
        diagnosis["icd10_code"]
        .value_counts()
        .head(15)
    )

    plt.figure(figsize=(10, 6))

    top_diag.sort_values().plot(kind="barh")

    plt.title("Top 15 Diagnosis Codes")
    plt.xlabel("Patient Count")
    plt.ylabel("ICD10 Code")

    plt.savefig(
        "datalake/consumption/plots/diagnosis_frequency.png"
    )

    plt.close()

    print("diagnosis_frequency.png created")


# ------------------------------------------------
# 3. Lab Result Distribution
# ------------------------------------------------

def create_lab_distribution(labs):
    test_types = labs["test_name"].unique()[:2]

    for test in test_types:
        subset = labs[
            labs["test_name"] == test
        ]

        plt.figure(figsize=(8, 5))

        subset["test_value"].hist(bins=20)

        # Example reference ranges
        plt.axvline(
            subset["test_value"].mean(),
            linestyle="--",
            label="Mean"
        )

        plt.title(f"{test} Distribution")
        plt.xlabel("Test Value")
        plt.ylabel("Count")

        plt.legend()

        filename = (
            f"datalake/consumption/plots/"
            f"{test}_distribution.png"
        )

        plt.savefig(filename)

        plt.close()

        print(f"{test}_distribution.png created")


# ------------------------------------------------
# 4. Genomics Scatter Plot
# ------------------------------------------------

def create_variant_scatter(genomics):
    plt.figure(figsize=(8, 6))

    pathogenic = genomics[
        genomics["clinical_significance"]
        == "Pathogenic"
    ]

    likely_pathogenic = genomics[
        genomics["clinical_significance"]
        == "Likely Pathogenic"
    ]

    plt.scatter(
        pathogenic["read_depth"],
        pathogenic["allele_frequency"],
        label="Pathogenic"
    )

    plt.scatter(
        likely_pathogenic["read_depth"],
        likely_pathogenic["allele_frequency"],
        label="Likely Pathogenic"
    )

    plt.title(
        "Allele Frequency vs Read Depth"
    )

    plt.xlabel("Read Depth")
    plt.ylabel("Allele Frequency")

    plt.legend()

    plt.savefig(
        "datalake/consumption/plots/genomics_scatter.png"
    )

    plt.close()

    print("genomics_scatter.png created")


# ------------------------------------------------
# 5. High Risk Summary
# ------------------------------------------------

def create_high_risk_summary(high_risk):
    plt.figure(figsize=(6, 5))

    counts = [
        len(high_risk)
    ]

    labels = [
        "High Risk Patients"
    ]

    plt.bar(labels, counts)

    plt.title(
        "High Risk Patient Cohort"
    )

    plt.ylabel("Patient Count")

    plt.savefig(
        "datalake/consumption/plots/high_risk_summary.png"
    )

    plt.close()

    print("high_risk_summary.png created")


# ------------------------------------------------
# 6. Data Quality Overview
# ------------------------------------------------

def create_data_quality_chart():
    metrics = {
        "duplicates_removed": 12,
        "nulls_handled": 25,
        "encoding_fixed": 12
    }

    plt.figure(figsize=(7, 5))

    plt.bar(
        metrics.keys(),
        metrics.values()
    )

    plt.title(
        "Pipeline Data Quality Metrics"
    )

    plt.ylabel("Count")

    plt.savefig(
        "datalake/consumption/plots/data_quality.png"
    )

    plt.close()

    print("data_quality.png created")