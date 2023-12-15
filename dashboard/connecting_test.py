import snowflake.connector
import streamlit as st

account = 'gb72399'
username = 'khanghoang12'
password = 'Khang12102003@'

conn = snowflake.connector.connect(
    user= 'khanghoang12',
    password = 'Khang12102003@',
    account= 'gb72399',
    warehouse= "COMPUTE_WH",
    database= "DATA_LAKE",
    schema= "PUBLIC_TEST_HH",
    role= "ACCOUNTADMIN",
)