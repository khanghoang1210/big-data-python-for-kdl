
from pyspark.sql.functions import regexp_replace, col, when, udf, expr
from pyspark.sql.types import FloatType
from udfs import convert_monetary_values

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
convert_monetary_udf = udf(convert_monetary_values, FloatType())
# Function to read data from Snowflake
def preprocess_data_and_write_to_silver(spark, sfOptions, table_name):
    df = spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .load()


# Function to process general data
    if table_name == "movie_revenue":
        # Process 'REVENUE' column
        df = df.withColumn('REVENUE', regexp_replace('REVENUE', ',', '').cast(FloatType()))

        # Replace '-' with '0' in 'GROSS_CHANGE_PER_DAY' and 'GROSS_CHANGE_PER_WEEK'
        df = df.withColumn('GROSS_CHANGE_PER_DAY', when(col('GROSS_CHANGE_PER_DAY') == '-', 0)\
                           .otherwise(regexp_replace('GROSS_CHANGE_PER_DAY', '%', '').cast(FloatType())))
        df = df.withColumn('GROSS_CHANGE_PER_WEEK', when(col('GROSS_CHANGE_PER_WEEK') == '-', 0)\
                           .otherwise(regexp_replace('GROSS_CHANGE_PER_WEEK', '%', '').cast(FloatType())))
        df.write\
            .format(SNOWFLAKE_SOURCE_NAME)\
            .options(**sfOptions)\
            .option("dbtable", table_name)\
            .mode("append")\
            .save()


    if table_name == "movies_detail":
        # Process 'RATING' column
        df = df.withColumn('RATING', regexp_replace('RATING', '[^0-9.]', '').cast('float'))

        # Process 'BUDGET' and 'WORLDWIDE_GROSS' columns
        df = df.withColumn('BUDGET', convert_monetary_udf('BUDGET'))
        df = df.withColumn('WORLDWIDE_GROSS', convert_monetary_udf('WORLDWIDE_GROSS'))

        # Process 'GENRE' column
        df = df.withColumn('GENRE', expr("regexp_replace(GENRE, 'Its Me, Margaret.?', '')"))

    print(df.show())
    print(df.printSchema())
