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

sub_cur = conn.cursor()
sub_sql = 'select * from data_lake.SILVER.MOVIES_DETAIL'
sub_cur.execute(sub_sql)
sub_data = sub_cur.fetch_pandas_all()
sub_df = pd.DataFrame(sub_data)

print(sub_df)