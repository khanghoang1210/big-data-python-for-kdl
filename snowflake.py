import snowflake.connector
import userandpassword as uap

conn = snowflake.connector.connect(
    user=uap.username,
    password=uap.password,
    # account="hwtmedy-oo41311",
    account="sh27034.ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="DATA_LAKE",
    schema="PUBLIC_TEST_HH",
    role="ACCOUNTADMIN",
)

cur = conn.cursor()
sql = "select * from data_lake.public_test_hh.analyst"
cur.execute(sql)
data = cur.fetch_pandas_all()
print(data.to_string())