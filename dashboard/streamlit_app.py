import snowflake.connector
import streamlit as st
import pandas as pd
import plost

# import matplotlib.pyplot as plt

# with open("user_account.txt", "r") as file:
#     account, user, password = file.readlines()
#     account = account.rstrip()
#     user = user.rstrip()

id_col = "ID"
title_col = "TITLE"
week_col = "WEEK"
rank_col = "RANK_CHANGE"
rating_col = "RATING"
revenue_col = "WEEKLY_GROSS_REVENUE"
gross_col = "GROSS_CHANGE_PER_WEEK"

conn = snowflake.connector.connect(
    user="khanghoang12",
    password="Khang12102003@",
    account="ALYUXMA-GB72399",
    warehouse="COMPUTE_WH",
    database="DATA_LAKE",
    schema="GOLD",
    role="ACCOUNTADMIN",
)

cur = conn.cursor()
sql = "select * from data_lake.GOLD.WEEKLY_MOVIE_REPORT"
cur.execute(sql)
data = cur.fetch_pandas_all()
data_frame = pd.DataFrame(data)


def month_list_detection():
    pass


def week_list_detection():
    week_list = []

    for i in range(len(data_frame["WEEK"])):
        if data_frame["WEEK"].iloc[i] not in week_list:
            week_list.append(data_frame["WEEK"].iloc[i])

    return week_list


def period_detection():
    period_list = []
    if period == "week":
        period_list = week_list_detection()
    else:
        period_list = month_list_detection()

    return tuple(period_list)


team_name = "team_name"
designer_name = "JR"

st.set_page_config(
    page_title="IMDB Dashboard",
    layout="wide",
    page_icon="logo.png",
    initial_sidebar_state="auto",
)

with open("style.css") as file:
    st.markdown(f"<style>{file.read()}</style>", unsafe_allow_html=True)

st.sidebar.header("IMDB Dashboard")

st.sidebar.subheader("")
period = st.sidebar.selectbox("Time by", ("week", "month"))
detected_period = period_detection()
choosen_period = st.sidebar.selectbox(f"Choose {period}", detected_period)
data_frame = data_frame[data_frame['WEEK'] == choosen_period]
top = st.sidebar.selectbox("Top ", ("highest", "lowest"))
num_show = st.sidebar.selectbox(
    "Show top ", ("10", "9", "8", "7", "6", "5", "4", "3", "2", "1")
)

st.sidebar.markdown(
    """
---
Data taken by [{}](https://www.youtube.com/watch?v=dQw4w9WgXcQ)
\nDesigned by [{}](https://www.youtube.com/watch?v=dQw4w9WgXcQ)
""".format(
        team_name, designer_name
    )
)


# taking data by period
def fill_data_by_period(df):
    pass


def show_1_top(df, col, type):
    if type == "highest":
        sorted_df = df.sort_values(by=col, ascending=False)
    else:
        sorted_df = df.sort_values(by=col, ascending=True)

    name = sorted_df[title_col].iloc[0]
    value = sorted_df[col].iloc[0]

    if str(col) == rank_col or str(col) == gross_col:
        return name, str(value) + "%"

    return name, str(value)


def show_n_top(df, num, col, type):
    num = int(num)
    if type == "highest":
        sorted_df = df.sort_values(by=col, ascending=False)
    else:
        sorted_df = df.sort_values(by=col, ascending=True)

    output_list = ""
    for i in range(num):
        output_list += "{0}. {1} - {2}: {3}\n".format(
            i + 1, sorted_df[title_col].iloc[i], col, sorted_df[col].iloc[i]
        )

    st.write("Top {} {}: ".format(num, type))
    st.write(output_list)


# Need uprade this function, search 'like' not 'exact'
def search_function(df, text, col):
    is_result_found = False
    result = ""
    text = str(text).upper()

    for i in range(len(df)):
        process_value = str(df[col].iloc[i]).upper()
        result = ""
        if process_value == text:
            result += "NAME: " + str(df[title_col].iloc[i]) + "\n"
            result += "RATING: " + str(df[rating_col].iloc[i]) + "\n"
            result += "REVENUE: " + str(df[revenue_col].iloc[i]) + "\n"
            result += "RANK CHANGE: " + str(df[rank_col].iloc[i]) + "%" + "\n"
            result += "GROSS CHANGE: " + str(df[gross_col].iloc[i]) + "%"

            is_result_found = True

            st.write(result)

    if not is_result_found:
        st.write("No Result Found !")


def main():
    st.header("🎥 IMDB DASHBOARD 🎞️")

    # DIV 1 -- list top div
    st.markdown("### Top {} value".format(top))

    col_1, col_1_blank = st.columns((9.5, 0.5))
    top_name, top_value = show_1_top(data_frame, rating_col, top)
    col_1.metric("----- Top rating", top_name, top_value)

    col_2, col_2_blank = st.columns((9.5, 0.5))
    top_name, top_value = show_1_top(data_frame, revenue_col, top)
    col_2.metric("----- Top revenue", top_name, top_value)

    col_3, col_3_blank = st.columns((9.5, 0.5))
    top_name, top_value = show_1_top(data_frame, rank_col, top)
    col_3.metric("----- Top rank change", top_name, top_value)

    col_4, col_4_blank = st.columns((9.5, 0.5))
    top_name, top_value = show_1_top(data_frame, gross_col, top)
    col_4.metric("----- Top gross change", top_name, top_value)

    # DIV 2
    c1, c2 = st.columns((6.5, 3.5))
    with c1:
        st.markdown("### Rating chart")
        st.bar_chart(data_frame[rating_col])

    with c2:
        st.markdown("### Top 5 {}".format(top))
        show_n_top(data_frame, num_show, rating_col, top)

    # DIV 3
    c1, c2 = st.columns((6.5, 3.5))
    with c1:
        st.markdown("### Revenue chart")
        # plost.bar_chart(
        #     data=data_frame.sort_values(by=revenue_col, ascending=False),
        #     bar=title_col,
        #     value=revenue_col,
        #     color="blue",
        #     width=600,
        #     use_container_width=True,
        # )
        st.bar_chart(data_frame[revenue_col])
    with c2:
        st.markdown("### Top 5 {}".format(top))
        show_n_top(data_frame, num_show, revenue_col, top)

    # DIV 4
    st.markdown("### Rank change chart")
    st.bar_chart(data_frame[rank_col])

    # DIV 5
    st.markdown("### Gross change chart")
    st.bar_chart(data_frame[gross_col])

    # DIV 6
    col, search_text = st.columns((3, 7))
    with col:
        col_name = st.selectbox("Column name:", ("TITLE", "ID"))
    with search_text:
        search_text_value = st.text_input("Search box: ")

    search_function(data_frame, search_text_value, str(col_name))
    st.write(data_frame)


main()

# streamlit run c:\Users\JR\Desktop\PJ\bigdata-for-movie-prediction\dashboard\streamlit_app.py