import pandas as pd

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
def process_weekly_df(sfOptions, spark):

    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                    .options(**sfOptions) \
                    .option("query",  "select * from movie_revenue")\
                    .load()

    # Xử lý cột 'REVENUE'
    #data = pd.DataFrame(df)d
    print(df.show())
    # df['revenue'] = df['revenue'].str.replace(',', '').astype(float)
    # print(df['revenue'])

    # # Thay thế giá trị '-' bằng 0 trong cột 'GROSS_CHANGE_PER_DAY' và 'GROSS_CHANGE_PER_WEEK'
    # df['GROSS_CHANGE_PER_DAY'] = df['GROSS_CHANGE_PER_DAY'].replace('-', '0')
    # df['GROSS_CHANGE_PER_WEEK'] = df['GROSS_CHANGE_PER_WEEK'].replace('-', '0')
    # # Xử lý cột 'GROSS_CHANGE_PER_DAY'
    # df['GROSS_CHANGE_PER_DAY'] = df['GROSS_CHANGE_PER_DAY'].str.replace('%', '').str.replace(',', '').astype(float)
    # df['GROSS_CHANGE_PER_DAY'].fillna(0, inplace=True)

    # # Xử lý cột 'GROSS_CHANGE_PER_WEEK'
    # df['GROSS_CHANGE_PER_WEEK'] = df['GROSS_CHANGE_PER_WEEK'].str.replace('%', '').str.replace(',', '').astype(
    #     float)
    # df['GROSS_CHANGE_PER_WEEK'].fillna(0, inplace=True)


def process_movie_df(df):
    #data = pd.DataFrame(df)
    print(df)
    #Process 'RATING' column: Convert to float
    df['RATING'] = df['RATING'].astype(str).str.extract('(\d+\.?\d*)').astype(float)

    # Function to convert monetary values
    def convert_monetary_values(value):
        if pd.isna(value):
            return 20e6  # Default value for nulls
        value = value.replace(' million', '').replace('$', '').replace(',', '')
        try:
            return float(value) * 1e6 if 'million' in value else float(value)
        except ValueError:
            return 0.0

    # Process 'BUDGET' and 'WORLDWIDE_GROSS' columns
    df['BUDGET'] = df['BUDGET'].apply(convert_monetary_values)
    df['WORLDWIDE_GROSS'] = df['WORLDWIDE_GROSS'].apply(convert_monetary_values)

    # Process 'GENRE' column: Handling specific text patterns
    df['GENRE'] = df['GENRE'].str.replace('Its Me, Margaret.?', '', regex=True)