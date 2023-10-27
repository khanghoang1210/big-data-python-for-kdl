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


# sfOptions = {
#     "sfURL": sfURL,
#     "sfAccount": sfAccount,
#     "sfUser": sfUser,
#     "sfPassword": sfPassword,
#     "sfDatabase": "DATA_LAKE",
#     "sfSchema": "PUBLIC",
#     "sfWarehouse": "COMPUTE_WH",
#     "sfRole": "ACCOUNTADMIN"
# }

# SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# # create table in snowflake
# if table_name == "movie_revenue":
#     query = f""" CREATE TABLE IF NOT EXISTS movie_revenue (
#                 id varchar,
#                 rank integer,
#                 revenue varchar,
#                 gross_change_per_day varchar,
#                 gross_change_per_week varchar,
#                 crawled_date date,
#                 primary key(crawled_date, id)
#             )
#             """
# elif table_name == "movies_detail":
#         query = f""" CREATE TABLE IF NOT EXISTS movies_detail (
#                 id varchar,
#                 title varchar,
#                 duration varchar,
#                 rating varchar,
#                 director varchar,
#                 budget varchar,
#                 worldwide_gross varchar,
#                 genre varchar,
#                 crawled_date date,
#                 primary key(id)
#             )
#             """
# spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, query)


# # write data into data lake on snowflake
# jdbcDF.write\
#     .format(SNOWFLAKE_SOURCE_NAME)\
#     .options(**sfOptions)\
#     .option("dbtable", table_name)\
#     .mode('overwrite')\
#     .save()