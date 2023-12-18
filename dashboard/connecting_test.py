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

<<<<<<< HEAD
# fill data by week

week_list = []

for i in range(len(data_frame['WEEK'])):
    if data_frame['WEEK'].iloc[i] not in week_list:
        week_list.append(data_frame['WEEK'].iloc[i])

        print(data_frame['WEEK'].iloc[i])

print(week_list)
# proccessed_df = data_frame[data_frame['WEEK'] == 1]
# print(data_frame)
# print(proccessed_df)
=======
print(sub_df)
>>>>>>> dev-khoi
