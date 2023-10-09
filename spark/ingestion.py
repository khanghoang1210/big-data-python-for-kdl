from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local')\
    .appName("Snowflake Data Lake Writer") \
    .config("spark.jars", "./spark/utils/postgresql-42.6.0.jar")\
    .getOrCreate()


jdbcDF = spark.read \
              .format("jdbc") \
              .option("url", "jdbc:postgresql://localhost:5432/postgres") \
              .option("dbtable", "movie_revenue") \
              .option("user", "postgres") \
              .option("password", "khang12102003") \
              .option("driver", "org.postgresql.Driver") \
              .load()

jdbcDF.show()

sfOptions = {
    "sfURL": "https://app.snowflake.com/ytymwtq/bu51995/",
    "sfAccount": "bu51995",
    "sfUser": "khanghoang12",
    "sfPassword": "Khang12102003@",
    "sfDatabase": "data_lake",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}