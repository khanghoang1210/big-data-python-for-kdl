from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local')\
    .appName("Snowflake Data Lake Writer") \
    .config("spark.jars",
             "./spark/lib/postgresql-42.6.0.jar, ./spark/lib/spark-snowflake_2.12-2.9.0-spark_3.1.jar")\
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
    "sfURL": "ytymwtq-bu51995.snowflakecomputing.com",
    "sfAccount": "BU51995",
    "sfUser": "khanghoang12",
    "sfPassword": "Khang12102003@",
    "sfAuthenticator": "oauth",
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