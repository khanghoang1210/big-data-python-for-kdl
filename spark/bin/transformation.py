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
## Report:
    # 1.ID
    # 2.Title
    # 3.month_revenue
    # 4.total_revenue
    # 5.rank_change
# Set logger 
# logging.config.fileConfig(fname='./spark/config/logging.conf')
# logger = logging.getLogger(__name__)

# Declare private variables

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
    print(sfOptions)

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
    df_joined = movie_revenue.join(movies_detail, movie_revenue.ID == movies_detail.ID).drop(movies_detail.CRAWLED_DATE)
    print(df_joined.show())
    # # Transform datatype of columns
    weekly_revenue = movie_revenue.groupBy("ID").agg(sum("REVENUE").alias("total_revenue"))
    df = movies_detail.select(col("ID"), col("TITLE"))
    

    # Get available columns
    # df = df_movies.select(col("movie_id").alias("id"), col("title"))
    # 

    # # Join main columns 
    # df_joined = df_revenue.join(df_movies, df_revenue.id == df_movies.movie_id).drop(df_movies.crawled_date)

    # # Get latest week
    max_crawled_date = df_joined.select(max(col('CRAWLED_DATE'))).collect()[0][0]
    print(max_crawled_date)
    # seven_days_ago = max_crawled_date - expr("INTERVAL 7 DAYS")
    # df_filtered = df_joined.filter(col('crawled_date') >= seven_days_ago)

    # # Get week revenue of each movie
    # df_week_revenue = df_filtered.groupBy('movie_id').agg(sum('revenue').alias('week_revenue'))

    # # Join dataframe
    # df_res = df.join(total_revenue, df["id"]==total_revenue["id"],"right")\
    #         .join(df_week_revenue, df.id==df_week_revenue.movie_id).drop("movie_id")\
    #                                                                 .drop(total_revenue["id"])

    # #df_res.show()
    # #df_filtered.show(300)

    # # Define window function
    # window_spec = Window.orderBy(col("crawled_date")).partitionBy("id")

    # # Get max crawled date for each movie
    # df_max_date = df_revenue.groupBy("id").agg(max("crawled_date").alias("max_date"))
    # df_max_date = df_max_date.withColumnRenamed("id","movie_id")

    # # Get previous date from max date
    # df_with_max_date = df_revenue.withColumn("max_crawled_date", max("crawled_date").over(window_spec))
    # df_prev_date = df_with_max_date.withColumn("previous_date", lag("max_crawled_date").over(window_spec))\
    #                                .withColumn("prev_rank", lag("rank").over(window_spec))

    # # Join dataframe for calc rank change
    # df_result = df_prev_date.join(df_max_date).where((df_prev_date.crawled_date==df_max_date.max_date) & (df_prev_date['id']==df_max_date['movie_id']))
    # # Get rank change for each movie
    # rank_change = df_result.withColumn("rank_change",df_result.prev_rank-df_result.rank)
    # rank_change = rank_change.withColumn("rank_change", coalesce("rank_change", lit(0)))

    # #rank_change.show()

    # # Weekly movie revenue report
    # analysis = df_res.join(rank_change, df_res.id==rank_change.id).select(df_res["*"], rank_change["rank_change"])

    # analysis = analysis.withColumn("year", lit(year_created))\
    #                     .withColumn("month", lit(month_created))\
    #                     .withColumn("day", lit(day_created))
    
    # analysis.show(50)
    # logger.info("Genarated analysis report")

    # # Write to data warehouse
    # analysis.write.format("hive").mode("append").partitionBy("year", "month", "day").saveAsTable("reports.movies")
    #logger.info("Write to data warehouse completed.")
except Exception as exp:
    print("Error occur in method transfromation. Please check the Stack Trace, ", str(exp))
    raise
else:
    print("Transformation is completed.")