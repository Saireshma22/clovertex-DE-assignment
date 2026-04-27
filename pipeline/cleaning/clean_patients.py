import pandas as pd


def clean_patients(df):
    # Rename beta columns if they exist
    df = df.rename(columns={
        "patientID": "patient_id",
        "birthDate": "date_of_birth",
        "gender": "sex",
        "bloodType": "blood_group"
    })

    # Convert dict/object columns to string
    # because drop_duplicates cannot handle dict type
    for col in df.columns:
        df[col] = df[col].astype(str)

    # Remove duplicates safely
    df = df.drop_duplicates()

    # Fill missing age if present
    if "age" in df.columns:
        df["age"] = df["age"].replace("nan", pd.NA)
        df["age"] = pd.to_numeric(df["age"], errors="coerce")
        df["age"] = df["age"].fillna(df["age"].median())

    # Standardize gender values
    if "sex" in df.columns:
        df["sex"] = df["sex"].replace({
            "M": "Male",
            "F": "Female",
            "male": "Male",
            "female": "Female"
        })

    return df