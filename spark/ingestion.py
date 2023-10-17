from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

# Declare private variables
sfURL = os.getenv("sfURL")
sfAccount = os.getenv("sfAccount")
sfUser = os.getenv("sfUser")
sfPassword = os.getenv("sfPassword")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")

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
              .option("user", db_user) \
              .option("password", db_password) \
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

query = """ CREATE TABLE IF NOT EXISTS movie_revenue (
            id varchar,
            rank integer,
            revenue varchar,
            gross_change_per_day varchar,
            gross_change_per_week varchar,
            crawled_date date,
            primary key(crawled_date, id)
        )
        """
spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, query)

jdbcDF.write\
    .format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("dbtable", "movie_revenue")\
    .mode('overwrite')\
    .save()
