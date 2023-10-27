from pyspark.sql import SparkSession
import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
logger = logging.getLogger(__name__)


def read_data_from_postgre(spark, table_name, db_user, db_password):
    try:
        logger.info("read_data_from_postgres() is started!")
        jdbcDF = spark.read \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://localhost:5434/postgres") \
                    .option("dbtable", table_name) \
                    .option("user", db_user) \
                    .option("password", db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

        
    except Exception as exp:
        logger.error("Error in the method - read_data_from_postgres(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Read data from postgreSQL - read_data_from_postgre() is completed.")
        return jdbcDF



def ingest_data(spark, sfOptions, df, df_name):
    try:
        logger.info("Ingestion - ingest_data() is started ...")
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

        # create table in snowflake
        if df_name == "movie_revenue":
            query = f""" CREATE TABLE IF NOT EXISTS movie_revenue (
                        id varchar,
                        rank integer,
                        revenue varchar,
                        gross_change_per_day varchar,
                        gross_change_per_week varchar,
                        crawled_date date,
                        primary key(crawled_date, id)
                    )
                    """
        elif df_name == "movies_detail":
                query = f""" CREATE TABLE IF NOT EXISTS movies_detail (
                        id varchar,
                        title varchar,
                        duration varchar,
                        rating varchar,
                        director varchar,
                        budget varchar,
                        worldwide_gross varchar,
                        genre varchar,
                        crawled_date date,
                        primary key(id)
                    )
                    """
        
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, query)
        logger.info("All tables is in data lake!!")

        logger.info("Writting data to Snowflake table is started...")
        # write data into data lake on snowflake
        df.write\
            .format(SNOWFLAKE_SOURCE_NAME)\
            .options(**sfOptions)\
            .option("dbtable", df_name)\
            .mode('overwrite')\
            .save()
    except Exception as exp:
        logger.error(f"Error in the method of {df_name} dataframe - ingest_data(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The ingest_data() function for dataframe {df_name} is completed!!")