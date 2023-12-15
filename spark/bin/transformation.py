# Import libraries
from pyspark.sql.functions import *
import logging
import logging.config
import os
from dotenv import load_dotenv

# Load the Logging Configuration File
logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
logger = logging.getLogger(__name__)


load_dotenv()
sfURL = os.getenv("sfURL")
sfAccount = os.getenv("sfAccount")
sfUser = os.getenv("sfUser")
sfPassword = os.getenv("sfPassword")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

sf_options_silver = {
"sfURL": sfURL,
"sfAccount": sfAccount,
"sfUser": sfUser,
"sfPassword": sfPassword,
"sfDatabase": "DATA_LAKE",
"sfSchema": "SILVER",
"sfWarehouse": "COMPUTE_WH",
"sfRole": "ACCOUNTADMIN"
}
sf_options_gold = {
    "sfURL": sfURL,
    "sfAccount": sfAccount,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": "DATA_LAKE",
    "sfSchema": "GOLD",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}
def transform(spark):
    try:
        logger.info("Transform() is started...")
        create_table = f""" CREATE TABLE IF NOT EXISTS weekly_movie_report (
                            id varchar,
                            title varchar,
                            week integer,
                            rank_change integer,
                            rating float,
                            weekly_gross_revenue float,
                            gross_change_per_week float,
                            crawled_date date,
                            primary key(week, id)
                        )
                        """
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sf_options_gold, create_table)
        logger.info("Created table in gold zone.")

        logger.info("Transform data is starting...")
        # handling change data capture
        snowflake_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sf_options_gold) \
                .option("dbtable", "weekly_movie_report") \
                .load()
        record_count = snowflake_df.count()
        latest_week = snowflake_df.select(max(col('week'))).collect()[0][0]

        if latest_week == None:
            latest_week = 0

        # Get latest value of crawled_date field
        if record_count == 0:
            query = f"(SELECT * FROM DATA_LAKE.SILVER.movie_revenue) AS tmp"
        else:
            last_crawled_date = snowflake_df.select("crawled_date")\
                .agg({"crawled_date": "max"}).collect()[0]["max(crawled_date)"]
            query = f"(SELECT * FROM DATA_LAKE.SILVER.movie_revenue WHERE (crawled_date > DATE '{last_crawled_date}')) AS tmp"

        # Load data from datalake to spark dataframe
        movies_detail = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sf_options_silver) \
                .option("dbtable", "movies_detail") \
                .load()
        movie_revenue = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sf_options_silver) \
                .option("dbtable", query) \
                .load()
        df_joined = movie_revenue.join(movies_detail, movie_revenue.ID == movies_detail.ID)\
            .drop(movies_detail.CRAWLED_DATE).drop(movie_revenue.ID)

        df = movies_detail.select(col("ID"), col("TITLE"), col("RATING"))
        

        # Get latest day in week to calc sum revenue
        max_crawled_date = df_joined.select(max(col('CRAWLED_DATE'))).collect()[0][0]
        min_crawled_date = df_joined.select(min(col('CRAWLED_DATE'))).collect()[0][0]

        seven_days_ago = max_crawled_date - expr("INTERVAL 7 DAYS")
        df_filtered = df_joined.filter(col('crawled_date') >= seven_days_ago)

        # Get week revenue of each movie
        df_weekly_revenue = df_filtered.groupBy('ID').agg(sum('REVENUE').alias('weekly_gross_revenue'))

        df_week = movie_revenue.groupBy("id").agg(max("crawled_date").alias("max_date"))
        df_week = df_week.withColumn(
            "min_date", 
            when(df_week.max_date - expr("INTERVAL 6 DAYS") >= 
                 min_crawled_date, df_week.max_date - expr("INTERVAL 6 DAYS"))
            .otherwise(min_crawled_date))
        df_week = df_week.withColumnRenamed("id", "week_id").withColumnRenamed("max_date", "week_max_date")
        movie_revenue = movie_revenue.withColumnRenamed("ID", "revenue_id")\
                                .withColumnRenamed("CRAWLED_DATE", "revenue_crawled_date")

        df_max_date = df_week.join(movie_revenue, 
                                    (df_week.week_max_date == movie_revenue.revenue_crawled_date) &
                                    (df_week.week_id == movie_revenue.revenue_id)).drop(df_week.min_date)\
                                    .drop(movie_revenue.revenue_id)
        df_min_date = df_week.join(movie_revenue, 
                                    (df_week.min_date == movie_revenue.revenue_crawled_date) &
                                    (df_week.week_id == movie_revenue.revenue_id))\
                                    .drop(df_week.week_max_date).drop(movie_revenue.revenue_id)
        df_min_date = df_min_date.withColumnRenamed("RANK", "rank_min_date")

        df_rank = df_max_date.join(df_min_date, df_min_date.week_id==df_max_date.week_id)\
                            .withColumn("rank_change", df_min_date.rank_min_date-df_max_date.RANK)\
                            .drop(df_min_date.week_id).drop(df_min_date.GROSS_CHANGE_PER_WEEK)
        
        df_rank_change = df_rank.select("week_id",
                                         "week_max_date","min_date", 
                                         "RANK","rank_min_date", 
                                         "rank_change", "GROSS_CHANGE_PER_WEEK")
        df = df.join(df_rank_change, df.ID==df_rank_change.week_id)\
                                            .join(df_weekly_revenue, df.ID ==df_weekly_revenue.ID)\
                                            .drop(df_rank_change.week_id)\
                                            .drop(df.ID)
                                            
        df = df.withColumn("week", lit(latest_week + 1))
        df = df.select("ID", "TITLE", "week", 
                       "rank_change", "rating", "weekly_gross_revenue", 
                       "GROSS_CHANGE_PER_WEEK", "week_max_date")

    except Exception as exp:
        logger.error("Error occur in method transfromation. Please check the Stack Trace, ", str(exp))
        raise
    else:
        logger.info("Transformation is completed.")
        return df
    
def load(spark, df, df_name):
    try:
        df.write\
                .format(SNOWFLAKE_SOURCE_NAME)\
                .options(**sf_options_gold)\
                .option("dbtable", df_name)\
                .mode("append")\
                .save()
        logger.info("Load data into gold zone - load() is started...")
    except Exception as exp:
        logger.error("Error occur in method load(). Please check the Stack Trace, ", str(exp))
        raise
    else:
        logger.info(f"Load data into gold zone - load() is completed. Dataframe {df_name} is in gold zone.")