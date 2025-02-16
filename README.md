## Building ELT Pipeline with Airflow for Sydney Airbnb Listings Dataset
### Project Objective
This project aims to build an end-to-end Extraction, Loading, and Transformation (ELT) pipeline for the Sydney Airbnb listings dataset. Using Apache Airflow for orchestration and scheduling tasks, dbt (Data Build Tool) will handle data transformations, creating a Medallion architecture in a PostgreSQL-based data warehouse. The project also includes business analysis, generating insights into Sydney’s Airbnb market.

### Analysis Procedures:
#### Setup (Part 0):

Setting up Google Cloud environment for storing the datasets.
Configuring DBeaver for database management.
Setting up dbt for transforming the data and building a data warehouse.
#### Data Ingestion with Airflow (Part 1):

Preparing the datasets and schema for the ingestion process.
Building an Airflow Directed Acyclic Graph (DAG) for orchestrating the data ingestion tasks.
#### Data Transformation with dbt (Part 2):

Configuring the data source within dbt.
Creating the Bronze, Silver, and Gold layers using dbt.
Implementing Slowly Changing Dimensions (SCD) Type 2 for tracking historical data.
Deploying dbt jobs for data transformation.
#### End-to-End Orchestration with Airflow (Part 3):

Configuring Apache Airflow to handle the full pipeline orchestration.
Ensuring that the ELT pipeline runs smoothly, with automated tasks and fault tolerance.
#### Ad-Hoc Analysis (Part 4):

Performing business analysis based on the structured data in the data warehouse.
Generating key insights into the Sydney Airbnb market, including the relationship between listing types, revenue, and neighborhood characteristics.
#### Conclusion (Part 5):

Summarizing the approach used to build the ELT pipeline and data warehouse.
Discussing the insights gained through ad-hoc analysis, such as identifying trends in the Sydney Airbnb market, including popular listing types, host concentration, and key neighborhood factors influencing listing success.
### Conclusion
This project showcases the orchestration of a full ELT pipeline using Apache Airflow for task scheduling and management. The data transformation was efficiently handled using dbt, which resulted in a well-structured data warehouse based on the Medallion architecture (Bronze, Silver, Gold layers). PostgreSQL served as the storage solution. The business analysis, performed on the constructed data warehouse, provided valuable insights into Sydney’s Airbnb market, revealing factors that influence the success of listings in various neighborhoods.

### Acknowledgments
The data used in this project was sourced from:

Sydney Airbnb Listings Dataset: Scraped by Inside Airbnb.
Local Governance Areas (LGA) and Census Data: Provided by the Australian Bureau of Statistics (ABS).

This project was completed as part of the Big Data Engineering course at University of Technology Sydney (UTS).

