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

# fill data by week

week_list = []

for i in range(len(data_frame['WEEK'])):
    if data_frame['WEEK'].iloc[i] not in week_list:
        week_list.append(data_frame['WEEK'].iloc[i])

        print(data_frame['WEEK'].iloc[i])

# print(week_list)
proccessed_df = data_frame[data_frame['WEEK'] == 1]
# print(data_frame)
# print(proccessed_df)