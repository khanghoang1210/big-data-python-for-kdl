import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt


with open('user_account.txt', 'r') as file:
    user, password = file.readlines()
    user = user.rstrip()

conn = snowflake.connector.connect(
    user= user,
    password = password,
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
data_frame = pd.DataFrame(data)

def rating_bar_chart(df):
    sorted_rating_df = df.sort_values(by='RATING', ascending=False)

    plt.figure(figsize=(12, 6))

    plt.bar(range(len(sorted_rating_df['RATING'])), sorted_rating_df['RATING'], width=0.9, color="maroon", label="Rating")

    plt.ylim(0, 10)
    plt.xlabel('')
    plt.ylabel('Rating')
    plt.title("Bar chart for Rating")

    plt.show()

def main():
    rating_bar_chart(data_frame)


main()