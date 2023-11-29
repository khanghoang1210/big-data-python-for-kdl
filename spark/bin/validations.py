import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
logger = logging.getLogger(__name__)


def df_count(df, dfName):
    try:
        logger.info(f"The dataframe validation by count df_count() is started for dataframe {dfName}...")
        df_count = df.count()
        if df_count == 0:
            logging.info("No new data from database postgres!")
        logger.info(f"The data frame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by count df_count() is completed.")

def df_top10_rec(df, dfName):
    try:
        logger.info(f"The dataframe validation by count df_top10_rec() is started for dataframe {dfName}...")
        logger.info(f"The dataframe top 10 records are: ")
        df_pandas = df.limit(10).toPandas()
    
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by top 10 records df_top10_rec() is completed.")


def df_print_schema(df, dfName):
    try:
        logger.info(f"The dataframe schema validation is started for dataframe {dfName}...")
        schema = df.schema.fields
        logger.info(f"The data frame {dfName} schema is: ")
        for i in schema:
            logger.info(f"\t {i}")
    except Exception as exp:
        logger.error("Error in the method - df_print_schema(). Please check the Stack Trace, " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe schema validation is completed.")