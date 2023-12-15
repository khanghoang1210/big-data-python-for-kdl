# def show_top_5(data, col):
#     print('Top 5 highest {}:\t Top 5 lowest {}: '.format(col, col))
#     for i in range(5):
#         print('{0}. {1}\t {2}: {3}'.format(i + 1, data['name'].iloc[i], col, data[col].iloc[i]), end='')
#         print('\t {0}. {1}\t {2}: {3}'.format(i + 1, data['name'].iloc[5-i-1], col , data[col].iloc[5-i-1]))


# def rating_bar_chart(df):
#     sorted_rating_df = df.sort_values(by='rating', ascending=False)

#     plt.figure(figsize=(12, 6))

#     plt.bar(range(len(sorted_rating_df['rating'])), sorted_rating_df['rating'], width=0.9, color="maroon", label="Rating")

#     plt.xlabel('')
#     plt.ylabel('Rating')
#     plt.title("Bar chart for Rating")

#     plt.show()

#     show_top_5(sorted_rating_df.head(5), col='rating')


# def rankchange_bar_chart(df):
#     plt.figure(figsize=(12,6))

#     plt.bar(range(len(df['rank_change'])), df['rank_change'], width=0.9, color='blue')

#     plt.xlabel('')
#     plt.ylabel('rank change (%)')
#     plt.title('Bar chart for Rank Change')

#     plt.show()

#     # show_top_5()

import snowflake.connector
import streamlit as st
import pandas as pd
import plost
import matplotlib.pyplot as plt

with open('user_account.txt', 'r') as file:
    account, user, password = file.readlines()
    account = account.rstrip()
    user = user.rstrip()

id_col = 'ID'
title_col = 'TITLE'
week_col = 'WEEK'
rank_col = 'RANK_CHANGE_PER_WEEK'
gross_col = 'GROSS_CHANGE_PER_WEEK'
rating_col = 'RATING'
revenue_col = 'WEEKLY_REVENUE'
category_col = ''

conn = snowflake.connector.connect(
    user= user,
    password = password,
    account=account,
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

team_name = 'team_name'
designer_name = 'JR'

st.set_page_config(
    page_title="Movie Dashboard",
    layout="wide",
    page_icon="logo.png",
    initial_sidebar_state="auto",
)

with open("style.css") as file:
    st.markdown(f"<style>{file.read()}</style>", unsafe_allow_html=True)

st.sidebar.header("Movie Dashboard")

st.sidebar.subheader("")
period = st.sidebar.selectbox("Time by", ("week", "month"))
sort_value = str(st.sidebar.selectbox('Sort ascending', ('None', 'True', 'False')))
top = st.sidebar.selectbox('Top ', ('highest', 'lowest'))
num_show = st.sidebar.selectbox('Show top ', ('10', '9', '8', '7', '6', '5', '4', '3', '2', '1'))

st.sidebar.markdown("""
---
Data taken by [{}](https://www.youtube.com/watch?v=dQw4w9WgXcQ)
\nDesigned by [{}](https://www.youtube.com/watch?v=dQw4w9WgXcQ)
""".format(team_name, designer_name))

def show_1_top(df, col, type):
    if type == 'highest':
        sorted_df = df.sort_values(by=col, ascending=False)
    else:
        sorted_df = df.sort_values(by=col, ascending=True)

    name = sorted_df[title_col].iloc[0]
    value = sorted_df[col].iloc[0]

    if str(col) == rank_col or str(col) == gross_col:
        return name, str(value) + '%'

    return name, str(value)


def show_n_top(df, num, col, type):
    num = int(num)
    if type == 'highest':
        sorted_df = df.sort_values(by=col, ascending=False)
    else:
        sorted_df = df.sort_values(by=col, ascending=True)

    output_list = ''
    for i in range(num):
        output_list += '{0}. {1} - {2}: {3}\n'.format(i+1, sorted_df[title_col].iloc[i], col, sorted_df[col].iloc[i])

    st.write('Top {} {}: '.format(num, type))
    st.write(output_list)

def search_function(df, text, col):
    is_result_found = False
    result = ''
    text = str(text).upper()

    for i in range(len(df)):
        process_value = str(df[col].iloc[i]).upper()
        result = ''
        if process_value == text:
            result += 'NAME: ' + str(df[title_col].iloc[i]) + '\n'
            result += 'RATING: ' + str(df[rating_col].iloc[i]) + '\n'
            result += 'REVENUE: ' + str(df[revenue_col].iloc[i]) + '\n'
            result += 'RANK CHANGE: ' + str(df[rank_col].iloc[i]) + '%' + '\n'
            result += 'GROSS CHANGE: ' + str(df[gross_col].iloc[i]) + '%'

            is_result_found = True

            st.write(result)

    if not is_result_found:
        st.write('No Result Found !')

def draw_rating_barchart(df):
    # plost.bar_chart(
    #     data= df,
    #     bar= 'TITLE',
    #     value= 'RATING',
    #     color= 'maroon',
    #     width= 800,
    #     use_container_width= True
    # )
    st.bar_chart(df[rating_col])



def main():

    st.header('🎥 IMDB DASHBOARD 🎞️')

    # ROW 1
    st.markdown('### Top {} value'.format(top))
    col_1, col_2, col_3, col_4 = st.columns(4)
    top_name, top_value = show_1_top(data_frame, rating_col, top)
    col_1.metric('Top rating', top_name, top_value)
    top_name, top_value = show_1_top(data_frame, revenue_col, top)
    col_2.metric('Top revenue', top_name, top_value)
    top_name, top_value = show_1_top(data_frame, rank_col, top)
    col_3.metric('Top rank change', top_name, top_value)
    top_name, top_value = show_1_top(data_frame, gross_col, top)
    col_4.metric('Top gross change', top_name, top_value)

    # ROW 2
    c1, c2 = st.columns((6.5, 3.5))
    with c1:
        st.markdown('### Rating chart')

        if sort_value == 'True':
            sorted_dataframe = data_frame.sort_values(by= rating_col, ascending= True)
            draw_rating_barchart(sorted_dataframe)

        elif sort_value == 'False':
            sorted_dataframe = data_frame.sort_values(by= rating_col, ascending= False)
            draw_rating_barchart(sorted_dataframe)

        else:
            draw_rating_barchart(data_frame)


    with c2:
        st.markdown('### Top 5 {}'.format(top))
        show_n_top(data_frame, num_show, rating_col, top)

    # ROW 3
    c1, c2 = st.columns((6.5, 3.5))
    with c1:
        st.markdown('### Revenue chart')
        plost.bar_chart(
            data= data_frame.sort_values(by= revenue_col, ascending=False),
            bar = title_col,
            value= revenue_col,
            color= 'blue',
            width= 600,
            use_container_width= True
        )
        # st.bar_chart(data_frame[revenue_col])
    with c2:
        st.markdown('### Top 5 {}'.format(top))
        show_n_top(data_frame, num_show, revenue_col, top)

    # ROW 4
    # st.markdown('### Rank change chart')
    # plost.bar_chart(
    #     data= data_frame,
    #     bar='name',
    #     value= 'rank_change', 
    #     color= 'green',
    #     use_container_width= True
    # )

    # ROW 5
    # st.markdown('### Gross change chart')
    # st.bar_chart(data_frame['gross_change'])

    # Final ROW Rank change ans Gross change chart
    st.markdown('### Rank change - Gross change chart')
    plost.bar_chart(
        data= data_frame,
        bar= title_col,
        width= 1000,
        value= [rank_col]
    )

    # ROW 6
    col, search_text = st.columns((3, 7))
    with col:
        col_name = st.selectbox('Column name:', ('TITLE', 'ID'))
    with search_text:
        search_text_value = st.text_input('Search box: ')
    
    search_function(data_frame, search_text_value, str(col_name))
    # result = search_function(data_frame, search_text_value, str(col_name).lower())
    
    # if result is not None:
    #     st.write(result)
    # else:
    #     st.write("No results found")

    st.write(data_frame)

main()

# streamlit run c:\Users\JR\Desktop\PJ\bigdata-for-movie-prediction\dashboard\streamlit_app.py