def filter_genomics(df):
    if "clinical_significance" in df.columns:
        df = df[
            df["clinical_significance"].isin([
                "Pathogenic",
                "Likely Pathogenic"
            ])
        ]

    return df