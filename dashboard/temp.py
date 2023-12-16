import streamlit as st
import pandas as pd

data = {
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Edward'],
    'height': [5.5, 6.1, 5.7, 6.0, 5.8],
    'weight': [150, 170, 160, 180, 170]
}
df = pd.DataFrame(data)

sort_column = st.selectbox('Select column to sort by:', df.columns[1:])
df_sorted = df.sort_values(by=sort_column, ascending=False)

st.bar_chart(df_sorted['height'])

st.dataframe(df_sorted)

N = 20
name_list = df_sorted['name'].head(N).tolist()

st.write(name_list)