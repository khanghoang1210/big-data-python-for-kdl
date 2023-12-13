# import os
# from dotenv import load_dotenv
# import logging

# # Load the Logging Configuration File
# logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
# logger = logging.getLogger(__name__)



# def transformation(spark, snowflake_database, snowflake_schema, table_name):
#     try:
#         logger.info("Transform data - transformation() is started...")
#         movie_revenue = 
#     except Exception as exp:
#         logger.error("Error in the method - data_preprocess(). Please check the Stack Trace. " + str(exp),exc_info=True)  
    
#     else:
#         logger.info("Preprocess data - data_preprocess() is completed.")
#         return df


# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
import logging.config
import os
from dotenv import load_dotenv

try:
    load_dotenv()
    sfURL = os.getenv("sfURL")
    sfAccount = os.getenv("sfAccount")
    sfUser = os.getenv("sfUser")
    sfPassword = os.getenv("sfPassword")
    db_user = os.getenv("db_user")
    db_password = os.getenv("db_password")
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    sfOptions = {
    "sfURL": sfURL,
    "sfAccount": sfAccount,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": "DATA_LAKE",
    "sfSchema": "SILVER",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
    }

    #logger.info("Transformation is started...")
    # Create Spark Object
    spark = SparkSession.builder \
        .master('local')\
        .appName("Data pipeline for movies revenue analyst") \
        .config("spark.jars", "./spark/lib/spark-snowflake_2.12-2.12.0-spark_3.4.jar")\
        .getOrCreate()
    #logger.info("Spark object is created.")

    # receive argument

    # Load data from datalake to spark dataframe
    movies_detail = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", "movies_detail") \
            .load()
    movie_revenue = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", "movie_revenue") \
            .load()
    df_joined = movie_revenue.join(movies_detail, movie_revenue.ID == movies_detail.ID)\
        .drop(movies_detail.CRAWLED_DATE).drop(movie_revenue.ID)

    df = movies_detail.select(col("ID"), col("TITLE"))
    

    # Get latest day in week to calc sum revenue
    max_crawled_date = df_joined.select(max(col('CRAWLED_DATE'))).collect()[0][0]
    min_crawled_date = df_joined.select(min(col('CRAWLED_DATE'))).collect()[0][0]

    seven_days_ago = max_crawled_date - expr("INTERVAL 7 DAYS")
    df_filtered = df_joined.filter(col('crawled_date') >= seven_days_ago)

    # Get week revenue of each movie
    df_weekly_revenue = df_filtered.groupBy('ID').agg(sum('REVENUE').alias('weekly_gross_revenue'))
    print(df_weekly_revenue.show())

    window_spec = Window.orderBy(col("crawled_date")).partitionBy("id")
    df_week = movie_revenue.groupBy("id").agg(max("crawled_date").alias("max_date"))
    df_week = df_week.withColumn(
        "min_date", 
        when(df_week.max_date - expr("INTERVAL 6 DAYS") >= min_crawled_date, df_week.max_date - expr("INTERVAL 6 DAYS"))
        .otherwise(min_crawled_date))
    df_week = df_week.withColumnRenamed("id", "week_id").withColumnRenamed("max_date", "week_max_date")
    movie_revenue = movie_revenue.withColumnRenamed("ID", "revenue_id").withColumnRenamed("CRAWLED_DATE", "revenue_crawled_date")

    # Join with qualified column names
   

    print(df_week.show(100))

    df_max_date = df_week.join(movie_revenue, (df_week.week_max_date == movie_revenue.revenue_crawled_date) & (df_week.week_id == movie_revenue.revenue_id)).drop(df_week.min_date).drop(movie_revenue.revenue_id)
    df_min_date = df_week.join(movie_revenue, (df_week.min_date == movie_revenue.revenue_crawled_date) & (df_week.week_id == movie_revenue.revenue_id)).drop(df_week.week_max_date).drop(movie_revenue.revenue_id)
    df_min_date = df_min_date.withColumnRenamed("RANK", "rank_min_date")
    print(df_max_date.show())
    print(df_min_date.show())
    df_rank = df_max_date.join(df_min_date, df_min_date.week_id==df_max_date.week_id).withColumn("rank_change", df_min_date.rank_min_date-df_max_date.RANK).drop(df_min_date.week_id)
    df_rank_change = df_rank.select("week_id", "week_max_date","min_date", "RANK","rank_min_date", "rank_change")
    print(df_rank_change.show(50))
    print(df_rank_change.count())
    df = df.join(df_rank_change, df.ID==df_rank_change.week_id).join(df_weekly_revenue, df.ID ==df_weekly_revenue.ID)
    print(df.show(50))
    print(df.count())

except Exception as exp:
    print("Error occur in method transfromation. Please check the Stack Trace, ", str(exp))
    raise
else:
    print("Transformation is completed.")