from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from ingestion import read_data_from_postgre
from validations import df_count, df_print_schema ,df_top10_rec

import logging
import logging.config

logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
load_dotenv()

# Declare private variables
sfURL = os.getenv("sfURL")
sfAccount = os.getenv("sfAccount")
sfUser = os.getenv("sfUser")
sfPassword = os.getenv("sfPassword")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")

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
    
    # Reading data 
    movie_revenue_df = read_data_from_postgre(spark, "movie_revenue", db_user, db_password)
  
    movies_detail_df = read_data_from_postgre(spark, "movies_detail", db_user, db_password)

    # Validate data
    df_top10_rec(movie_revenue_df, "movie_revenue_df")
    df_count(movie_revenue_df, "movie_revenue_df")

    df_top10_rec(movies_detail_df, "movies_detail_df")
    df_count(movies_detail_df, "movies_detail_df")

    logging.info("main() is Compeleted.")
except Exception as exp:
        logging.error("Error occured in the main() method. Please check the Stack Trace, " + str(exp), exc_info=True)

   