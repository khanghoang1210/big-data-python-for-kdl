from pyspark.sql.functions import regexp_replace, col, when,expr,regexp_extract,isnull
from pyspark.sql.types import FloatType

def convert_to_number(column):
    # Trích xuất số và kiểm tra xem có chứa từ "million" không
    number = regexp_extract(column, r'(\d+\.?\d*)', 1).cast('float')
    is_million = when(col(column).contains('million'), 1e6).otherwise(1)
    return number * is_million


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# Function to read data from Snowflake
def data_preprocess(spark, sfOptions, table_name):
    df = spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .load()


# Function to process general data
    if table_name == "movie_revenue":
        # Process 'REVENUE' column
        df = df.withColumn('REVENUE', when(isnull('REVENUE'), 0).otherwise(regexp_replace('REVENUE', ',', '').cast(FloatType())))
        df = df.withColumn('GROSS_CHANGE_PER_DAY', when(col('GROSS_CHANGE_PER_DAY') == '-', 0)\
                           .otherwise(when(isnull('GROSS_CHANGE_PER_DAY'), 0).otherwise(regexp_replace('GROSS_CHANGE_PER_DAY', '%', '').cast(FloatType()))))
        df = df.withColumn('GROSS_CHANGE_PER_WEEK', when(col('GROSS_CHANGE_PER_WEEK') == '-', 0)\
                           .otherwise(when(isnull('GROSS_CHANGE_PER_WEEK'), 0).otherwise(regexp_replace('GROSS_CHANGE_PER_WEEK', '%', '').cast(FloatType()))))

        # Xóa tất cả các dòng có ít nhất một giá trị null
        df = df.dropna()

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

    print(df.show())
    print(df.printSchema())
