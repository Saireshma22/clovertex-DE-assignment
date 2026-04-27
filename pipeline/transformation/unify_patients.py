import pandas as pd

def unify_patients(alpha, beta):
    # Standardize alpha column names
    alpha.columns = alpha.columns.str.lower().str.strip()

    # Rename beta columns to match alpha
    beta = beta.rename(columns={
        "patientID": "patient_id",
        "birthDate": "date_of_birth",
        "gender": "sex",
        "bloodType": "blood_group"
    })

    # Convert beta columns to lowercase
    beta.columns = beta.columns.str.lower().str.strip()

    # Add missing columns in beta (optional)
    required_cols = [
        "patient_id",
        "date_of_birth",
        "sex",
        "blood_group",
        "site"
    ]

    for col in required_cols:
        if col not in beta.columns:
            beta[col] = None

    # Keep only important common columns
    selected_cols = [
        "patient_id",
        "date_of_birth",
        "sex",
        "blood_group",
        "site"
    ]

    alpha = alpha[selected_cols]
    beta = beta[selected_cols]

    # Combine both datasets
    patients = pd.concat(
        [alpha, beta],
        ignore_index=True
    )

    # Remove duplicates
    patients = patients.drop_duplicates(
        subset=["patient_id"]
    )

    return patients