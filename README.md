# 📊 FinTech Data Pipeline with Apache Airflow

This project is a complete data engineering pipeline for processing, transforming, and loading FinTech loan data using **Apache Airflow** and **PostgreSQL**, with visual insights rendered via **Apache Superset**.

---

## 🚀 Project Overview

This ETL (Extract-Transform-Load) pipeline performs the following:

1. **Extracts & Cleans Raw CSV Data**
2. **Combines Datasets**
3. **Imputes Missing Values and Encodes Categorical Variables**
4. **Generates New Features**
5. **Loads the Final Dataset into a PostgreSQL Database**

The pipeline is orchestrated using Apache Airflow and visualized via Superset dashboards.

---

## 🛠️ Tech Stack

- **Python** (Pandas, Sklearn, SQLAlchemy)
- **Apache Airflow** (ETL orchestration)
- **PostgreSQL** (Data warehouse)
- **Apache Superset** (Dashboarding)
- **Docker** (optional for containerization)

---

## 📁 Folder Structure

<pre lang="md"> ``` 
├── airflow/
│ ├── dags/
│ │ └── fintech_dag.py
│ └── data/
│ ├── fintech_data.csv
│ ├── states.csv
│ ├── fintech_clean.parquet
│ ├── fintech_combined.parquet
│ └── fintech_encoded.parquet
├── _functions/
│ └── cleaning.py
└── README.md ``` </pre>
