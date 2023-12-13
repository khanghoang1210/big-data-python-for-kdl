from pyspark.sql.functions import regexp_replace, col, when,expr,regexp_extract,isnull
from pyspark.sql.types import FloatType, DoubleType
from dotenv import load_dotenv
import logging
import os
load_dotenv()

# Load the Logging Configuration File
logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
logger = logging.getLogger(__name__)

# Declare private variables
sfURL = os.getenv("sfURL")
sfAccount = os.getenv("sfAccount")
sfUser = os.getenv("sfUser")
sfPassword = os.getenv("sfPassword")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")

def convert_to_number(column):
    # Trích xuất số và kiểm tra xem có chứa từ "million" không
    number = regexp_extract(column, r'(\d+\.?\d*)', 1).cast('float')
    is_million = when(col(column).contains('million'), 1e6).otherwise(1)
    return number * is_million


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# Function to read data from Snowflake
def data_preprocess(spark, snowflake_database, snowflake_schema, table_name):
    sfOptions = {
    "sfURL": sfURL,
    "sfAccount": sfAccount,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
    } 
    try:
        logger.info("Preprocessing data - data_preprocess() is started ...")
        df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", table_name) \
            .load()

    # Function to process general data
        if table_name == "movie_revenue":
            # Process 'REVENUE' column
            df = df.withColumn('REVENUE', when(isnull('REVENUE'), 0).otherwise(regexp_replace('REVENUE', ',', '').cast(DoubleType())))
            df = df.withColumn('GROSS_CHANGE_PER_DAY', when(col('GROSS_CHANGE_PER_DAY') == '-', 0)\
                            .otherwise(when(isnull('GROSS_CHANGE_PER_DAY'), 0).otherwise(regexp_replace('GROSS_CHANGE_PER_DAY', '%', '').cast(DoubleType()))))
            df = df.withColumn('GROSS_CHANGE_PER_WEEK', when(col('GROSS_CHANGE_PER_WEEK') == '-', 0)\
                            .otherwise(when(isnull('GROSS_CHANGE_PER_WEEK'), 0).otherwise(regexp_replace('GROSS_CHANGE_PER_WEEK', '%', '').cast(DoubleType()))))

            # Xóa tất cả các dòng có ít nhất một giá trị null
            df = df.dropna()
            print(df.printSchema())

        if table_name == "movies_detail":
            # Process 'RATING' column
            df = df.withColumn('RATING', regexp_replace('RATING', '[^0-9.]', '').cast('float'))

            # Process 'BUDGET' and 'WORLDWIDE_GROSS' columns
            df = df.withColumn('BUDGET', convert_to_number('BUDGET'))
            df = df.withColumn('WORLDWIDE_GROSS', convert_to_number('WORLDWIDE_GROSS'))
            # Remove rows containing specific string in any column
            condition = " or ".join([f"contains({col}, 'Its Me, Margaret.')" for col in df.columns])
            df = df.filter(f"not ({condition})")

            # Remove rows with excessive missing data
            df = df.dropna(thresh=len(df.columns) - 2)

            # Process 'GENRE' column
            df = df.withColumn('GENRE', expr("regexp_replace(GENRE, 'Its Me, Margaret.?', '')"))

    except Exception as exp:
        logger.error("Error in the method - data_preprocess(). Please check the Stack Trace. " + str(exp),exc_info=True)  
    
    else:
        logger.info("Preprocess data - data_preprocess() is completed.")
        return df

def write_data_to_silver_zone(spark, snowflake_database, snowflake_schema, df, table_name):
    sfOptions = {
    "sfURL": sfURL,
    "sfAccount": sfAccount,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
    }
    try:    
        logger.info("Writting data into silver zone - write_data_to_silver_zone() is started...")
        save_mode = ""
        if table_name == "movies_detail":
            save_mode = 'overwrite'
        elif table_name == "movie_revenue":
            save_mode = 'overwrite'
        df.write\
                .format(SNOWFLAKE_SOURCE_NAME)\
                .options(**sfOptions)\
                .option("dbtable", table_name)\
                .mode(save_mode)\
                .save()
        logger.info("Writting data to silver zone - write_data_to_silver_zone() is completed.")
    except Exception as exp:
        logger.error("Error in the method - write_data_to_silver_zone(). Please check the Stack Trace. " + str(exp),exc_info=True)  
    
    else:
        logger.info(f"Dataframe {table_name} is in silver zone - write_data_to_silver_zone() is completed.")
