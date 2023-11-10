# ETL for Ocean Cafe Analysis using Airflow
This repository contains an automated scheduler for data pre-processing using Apache Airflow for the Ocean Cafe in Canada. The project is designed to help the restaurant to improve sales and provide the best possible service to its customers.


## Project Overview
This ETL projects perform a batch processing with scheduler for automation using Airflow, starting from data extraction from PostgreSQL Database, data cleaning using pandas and data validation using Great Expectation to ensure data quality and consistency. Those process generate a clean data which will then saved to Elasticsearch to be analyzed and visualized using Kibana. This analysis aims to help Ocean Cafe to improve sales and provide the best possible service to its customers.
