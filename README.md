

# Data pipeline for movie analyst
This README would normally document whatever steps are necessary to get your application up and running.
## Pre-requisites
1. Install Spark version 3.4.1.
2. Install Airflow on Docker.
## What is this repository for? ###

* This repository for Python for Data Science Course - Pyspark and Big data

## How to run pipeline?
1. Use `docker compose up` to run airflow.
2. Set up `.env` file in local.
3. Run command `spark-submit --master local --jars ./spark/lib/postgresql-42.6.0.jar,./spark/lib/snowflake-jdbc-3.13.13.jar,/.//spark/lib/spark-snowflake_2.12-2.12.0-spark_3.4.jar ./spark/bin/main.py` to run full data pipeline.
## Schema
![schema drawio](https://github.com/khanghoang1210/bigdata-for-movie-analytic/assets/116246004/abc1458b-cf8a-4f43-aa1a-414e6d21cfaf)


## Delta Lake Architecture

![delta_lake](https://github.com/khanghoang1210/bigdata-for-movie-analytic/assets/116246004/fc2fac82-5670-4137-a235-d077c52ba597)
