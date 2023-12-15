import snowflake.connector
from dotenv import load_dotenv
import os
import streamlit as st
load_dotenv()
User = os.getenv('sfUser')
Password = os.getenv('sfPassword')
Account = os.getenv("sfAccount")

conn = snowflake.connector.connect(
    user=User,
    password=Password,
    account='alyuxma-gb72399',
    # account="sh27034.ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="DATA_LAKE",
    schema="PUBLIC_TEST_HH",
    role="ACCOUNTADMIN",
)

cur = conn.cursor()
sql = "select * from data_lake.public_test_hh.analyst"
cur.execute(sql)
analyst = cur.fetch_pandas_all()
print(analyst.to_string())


