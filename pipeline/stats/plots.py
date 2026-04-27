import matplotlib.pyplot as plt
import pandas as pd


def create_age_distribution(patients):
    if "date_of_birth" not in patients.columns:
        print("date_of_birth column not found")
        return

    # Convert DOB to year safely
    patients["date_of_birth"] = patients["date_of_birth"].astype(str)
    patients["birth_year"] = patients["date_of_birth"].str[:4]

    patients["birth_year"] = patients["birth_year"].replace(
        ["None", "nan", ""],
        pd.NA
    )

    patients = patients.dropna(subset=["birth_year"])

    patients["birth_year"] = pd.to_numeric(
        patients["birth_year"],
        errors="coerce"
    )

    patients = patients.dropna(subset=["birth_year"])

    plt.figure()
    patients["birth_year"].hist()

    plt.title("Age Distribution")
    plt.xlabel("Birth Year")
    plt.ylabel("Count")

    plt.savefig(
        "datalake/consumption/plots/age_distribution.png"
    )

    plt.close()

    print("age_distribution.png created")


def create_gender_distribution(patients):
    if "sex" not in patients.columns:
        print("sex column not found")
        return

    gender_counts = patients["sex"].value_counts()

    plt.figure()
    gender_counts.plot(kind="bar")

    plt.title("Gender Distribution")
    plt.xlabel("Gender")
    plt.ylabel("Count")

    plt.savefig(
        "datalake/consumption/plots/gender_distribution.png"
    )

    plt.close()

    print("gender_distribution.png created")


def create_diagnosis_chart(diagnosis):
    if "icd10_code" not in diagnosis.columns:
        print("icd10_code column not found")
        return

    top_diag = diagnosis["icd10_code"].value_counts().head(10)

    plt.figure()
    top_diag.plot(kind="bar")

    plt.title("Top Diagnoses")
    plt.xlabel("ICD10 Code")
    plt.ylabel("Count")

    plt.savefig(
        "datalake/consumption/plots/diagnosis_frequency.png"
    )

    plt.close()

    print("diagnosis_frequency.png created")


def create_lab_distribution(labs):
    if "test_value" not in labs.columns:
        print("test_value column not found")
        return

    plt.figure()
    labs["test_value"].hist()

    plt.title("Lab Test Distribution")
    plt.xlabel("Test Value")
    plt.ylabel("Count")

    plt.savefig(
        "datalake/consumption/plots/lab_distribution.png"
    )

    plt.close()

    print("lab_distribution.png created")


def create_variant_scatter(genomics):
    if "read_depth" not in genomics.columns or "allele_frequency" not in genomics.columns:
        print("Required genomics columns not found")
        return

    plt.figure()

    plt.scatter(
        genomics["read_depth"],
        genomics["allele_frequency"]
    )

    plt.title("Genomics Variant Scatter")
    plt.xlabel("Read Depth")
    plt.ylabel("Allele Frequency")

    plt.savefig(
        "datalake/consumption/plots/genomics_scatter.png"
    )

    plt.close()

    print("genomics_scatter.png created")