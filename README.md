# Real Estate Data Pipeline Project

This project builds an automated data pipeline to collect and process real estate transaction data across Taiwan.

The system crawls transaction records, processes them through ETL,
stores them in MySQL, and schedules workflows using Apache Airflow.

Total data processed:
- 365 districts in Taiwan
- 1,038,839 records

doc_manual/      → platform deployment manual
mysql/           → database schema & procedures
python/          → crawler and ETL scripts
README.md        → project overview

# How to Run
1. Setup Airflow & MySQL database on GCP
2. Run crawler scripts in Python
3. Configure Airflow DAG
4. Schedule ETL workflow
