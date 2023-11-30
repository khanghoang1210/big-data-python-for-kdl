import snowflake.connector

conn = snowflake.connector.connect(
    user="khanghoang12",
    password="Khang12102003@",
    account="hwtmedy-oo41311",
    # account="sh27034.ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="DATA_LAKE",
    schema="TEST_PUBLIC_HH",
    role="ACCOUNTADMIN",
)

cur = conn.cursor()
sql = "select * from data_lake.test_public_hh.analyst"
cur.execute(sql)
df = cur.fetch_pandas_all()
print(df.to_string())