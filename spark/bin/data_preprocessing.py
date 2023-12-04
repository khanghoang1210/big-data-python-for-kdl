from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, udf, expr
from pyspark.sql.types import FloatType, StringType

# Function to initialize Spark session
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Data Processing with Snowflake") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.3,net.snowflake:spark-snowflake_2.12:2.9.2-spark_3.1") \
        .getOrCreate()
    return spark

# Function to get Snowflake connection options
def get_snowflake_options():
    return {
        "sfURL": "<your_snowflake_account_url>",
        "sfUser": "<your_username>",
        "sfPassword": "<your_password>",
        "sfDatabase": "<your_database>",
        "sfSchema": "<your_schema>",
        "sfWarehouse": "<your_warehouse>",
        "sfRole": "<your_role>",
    }

# Function to read data from Snowflake
def read_snowflake_data(spark, options, table_name):
    df = spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**options) \
        .option("dbtable", table_name) \
        .load()
    return df

# Function to process general data
def process_data(df):
    # Process 'REVENUE' column
    df = df.withColumn('REVENUE', regexp_replace('REVENUE', ',', '').cast(FloatType()))

    # Replace '-' with '0' in 'GROSS_CHANGE_PER_DAY' and 'GROSS_CHANGE_PER_WEEK'
    df = df.withColumn('GROSS_CHANGE_PER_DAY', when(col('GROSS_CHANGE_PER_DAY') == '-', 0).otherwise(regexp_replace('GROSS_CHANGE_PER_DAY', '%', '').cast(FloatType())))
    df = df.withColumn('GROSS_CHANGE_PER_WEEK', when(col('GROSS_CHANGE_PER_WEEK') == '-', 0).otherwise(regexp_replace('GROSS_CHANGE_PER_WEEK', '%', '').cast(FloatType())))

    return df

# Spark UDF for converting monetary values
def convert_monetary_values(value):
    if value is None:
        return 20e6
    value = value.replace(' million', '').replace('$', '').replace(',', '')
    try:
        return float(value) * 1e6 if 'million' in value else float(value)
    except ValueError:
        return 0.0

convert_monetary_udf = udf(convert_monetary_values, FloatType())

# Function to process movie-specific data
def process_movie_data(df):
    # Process 'RATING' column
    df = df.withColumn('RATING', regexp_replace('RATING', '[^0-9.]', '').cast('float'))

    # Process 'BUDGET' and 'WORLDWIDE_GROSS' columns
    df = df.withColumn('BUDGET', convert_monetary_udf('BUDGET'))
    df = df.withColumn('WORLDWIDE_GROSS', convert_monetary_udf('WORLDWIDE_GROSS'))

    # Process 'GENRE' column
    df = df.withColumn('GENRE', expr("regexp_replace(GENRE, 'Its Me, Margaret.?', '')"))

    return df

# Main execution function
def main():
    spark = create_spark_session()
    snowflake_options = get_snowflake_options()
    table_name = "<your_table_name>"

    # Read data from Snowflake
    data = read_snowflake_data(spark, snowflake_options, table_name)

    # Process the data using both functions
    processed_data = process_data(data)
    processed_movie_data = process_movie_data(processed_data)


    spark.stop()

if __name__ == "__main__":
    main()
