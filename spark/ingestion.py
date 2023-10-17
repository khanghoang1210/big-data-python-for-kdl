from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

# Declare private variables
sfURL = os.getenv("sfURL")
sfAccount = os.getenv("sfAccount")
sfUser = os.getenv("sfUser")
sfPassword = os.getenv("sfPassword")

spark = SparkSession.builder \
    .master('local')\
    .appName("Snowflake Data Lake Writer") \
    .config("spark.jars",
             "./spark/lib/postgresql-42.6.0.jar, ./spark/lib/spark-snowflake_2.12-2.12.0-spark_3.4.jar")\
    .getOrCreate()


jdbcDF = spark.read \
              .format("jdbc") \
              .option("url", "jdbc:postgresql://localhost:5434/postgres") \
              .option("dbtable", "movie_revenue") \
              .option("user", "airflow") \
              .option("password", "airflow") \
              .option("driver", "org.postgresql.Driver") \
              .load()

jdbcDF.show()

sfOptions = {
    "sfURL": sfURL,
    "sfAccount": sfAccount,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": "DATA_LAKE",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

jdbcDF.write\
    .format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("dbtable", "TEST")\
    .mode('append')\
    .save()

# df = spark.read.format(SNOWFLAKE_SOURCE_NAME)\
#                 .options(**sfOptions)\
#                 .option('query', "select * from data_lake.public.test")\
#                 .load()
# df.show()