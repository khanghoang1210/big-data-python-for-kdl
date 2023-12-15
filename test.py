import streamlit as st
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

load_dotenv()
User = os.getenv('sfUser')
Password = os.getenv('sfPassword')
Account = os.getenv("sfAccount")
# Initialize connection.
conn = st.connection("snowflake")

# Load the table as a dataframe using the Snowpark Session.
@st.cache_data
def load_table():
    session = conn.session(
        user=User,
        password=Password,
        account='alyuxma-gb72399',
        # account="sh27034.ap-southeast-1",
        warehouse="COMPUTE_WH",
        database="DATA_LAKE",
        schema="PUBLIC_TEST_HH",
        role="ACCOUNTADMIN",
    )
    return session.table("DATA_LAKE.PUBLIC_TEST_HH.ANALYST").to_pandas()

df = load_table()

# Print results.
for row in df.itertuples():
    st.write(f"{row.NAME} has a :{row.PET}:")