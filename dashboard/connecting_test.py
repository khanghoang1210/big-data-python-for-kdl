import snowflake.connector
import pandas as pd

account = 'gb72399'
username = 'khanghoang12'
password = 'Khang12102003@'

conn = snowflake.connector.connect(
    user= 'khanghoang12',
    password = 'Khang12102003@',
    account= 'ALYUXMA-GB72399',
    warehouse= "COMPUTE_WH",
    database= "DATA_LAKE",
    schema= "GOLD",
    role= "ACCOUNTADMIN",
)

cur = conn.cursor()
sql = "select * from data_lake.GOLD.WEEKLY_MOVIE_REPORT"
cur.execute(sql)
data = cur.fetch_pandas_all()
data_frame = pd.DataFrame(data)

print(data_frame)