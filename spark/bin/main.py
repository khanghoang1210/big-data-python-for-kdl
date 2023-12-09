from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from ingestion import read_data_from_postgre, ingest_data, create_snowflake_table
from validations import df_count, df_print_schema ,df_top10_rec
from data_preprocessing import data_preprocess

import logging
import logging.config

logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
load_dotenv()

database_name = ""
schema_name = ""

# Declare private variables
sfURL = os.getenv("sfURL")
sfAccount = os.getenv("sfAccount")
sfUser = os.getenv("sfUser")
sfPassword = os.getenv("sfPassword")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")

# Define connection in Snowflake
sfOptions = {
"sfURL": sfURL,
"sfAccount": sfAccount,
"sfUser": sfUser,
"sfPassword": sfPassword,
"sfDatabase": "DATA_LAKE",
"sfSchema": "BRONZE",
"sfWarehouse": "COMPUTE_WH",
"sfRole": "ACCOUNTADMIN"
}

# create spark object
try:
    logging.info("Main is started!")
    logging.info("Creating spark object!")
    spark = SparkSession.builder \
        .master('local')\
        .appName("Data pipeline for movies revenue analyst") \
        .config("spark.jars",
                "./spark/lib/postgresql-42.6.0.jar, ./spark/lib/spark-snowflake_2.12-2.12.0-spark_3.4.jar")\
        .getOrCreate()
    logging.info("Spark object is created.")

    create_snowflake_table(spark,"DATA_LAKE", "BRONZE", "movie_revenue")
    create_snowflake_table(spark, "DATA_LAKE", "BRONZE", "movies_detail")
    # Reading data 
    movie_revenue = read_data_from_postgre(spark, "movie_revenue", db_user, db_password, "DATA_LAKE", "BRONZE")
  
    movies_detail = read_data_from_postgre(spark, "movies_detail", db_user, db_password, "DATA_LAKE", "BRONZE")


    # Validate data
    df_top10_rec(movie_revenue, "movie_revenue")
    df_count(movie_revenue, "movie_revenue")
    

    df_top10_rec(movies_detail, "movies_detail")
    df_count(movies_detail, "movies_detail")


    # write movie_revenue data frame into data lake
    ingest_data(spark,"DATA_LAKE", "BRONZE",movie_revenue, "movie_revenue")

    # write movies_detail data frame into data lake
    ingest_data(spark,"DATA_LAKE", "BRONZE", movies_detail, "movies_detail")

    # preprocessing data
    data_preprocess_and_write_to_silver(spark, "DATA_LAKE","SILVER", "movie_revenue")
    data_preprocess_and_write_to_silver(spark, "DATA_LAKE","SILVER", "movies_detail")
    #process_movie_df(movies_detail)

    logging.info("main() is Compeleted.")
except Exception as exp:
        logging.error("Error occured in the main() method. Please check the Stack Trace, " + str(exp), exc_info=True)