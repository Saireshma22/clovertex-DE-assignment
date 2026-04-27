# Clovertex Data Engineering Assignment

## Project Overview

This project is a healthcare and genomics data pipeline built using Python, Pandas, Docker, and GitHub Actions.

The goal is to ingest data from 3 hospital sites in different formats (CSV, JSON, Parquet), clean and standardize the data, store it in a data lake structure, generate analytics outputs, detect anomalies, and create visualizations.

---

## Data Lake Structure

The project follows 3 layers:

### Raw Layer

Stores original untouched files.

### Refined Layer

Stores cleaned and transformed parquet files.

### Consumption Layer

Stores analytics-ready outputs and plots.

Each layer contains a `manifest.json` file with:

* file name
* row count
* schema
* processing timestamp
* SHA-256 checksum

---

## Data Cleaning

For patient datasets:

* standardized column names
* removed duplicates
* handled null values
* converted nested JSON fields to string
* standardized gender values

Example:

* patientID → patient_id
* birthDate → date_of_birth
* gender → sex

This made data consistent across all hospital sources.

---

## Genomics Filtering

Filtered genomics data using:

* clinical significance = Pathogenic
* allele frequency < 0.05

This helps focus only on important high-risk variants.

---

## Analytics Outputs

Created these files in consumption layer:

* patient_summary.parquet
* lab_statistics.parquet
* diagnosis_frequency.parquet
* variant_hotspots.parquet
* high_risk_patients.parquet
* anomaly_flags.parquet

Example:

High-risk patients were identified using:

* HbA1c > 7

---

## Visualizations

Created plots:

* age distribution
* gender distribution
* diagnosis frequency
* lab distribution
* genomics scatter plot

These help in quick analysis.

---

## Docker

Used Docker to run the full pipeline using:

```bash id="n5n7v8"
docker compose up --build
```

This ensures the project runs successfully in any environment.

---

## CI/CD

Used GitHub Actions for:

* dependency installation
* lint checking
* Docker build validation

This keeps CI green on every push.

---

## Most Important Decision

The most important decision was standardizing schema differences before analytics.

Since data came from different hospitals with different formats and field names, I unified the schema first so that all downstream processing became easier, reliable, and scalable.
