import pandas as pd
import streamlit as st
import streamlit_nested_layout
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas


st.set_page_config(
    layout="wide"
)

st.markdown(
            f'''
            <style>
                .reportview-container .sidebar-content {{
                    padding-top: {1}rem;
                }}
                .reportview-container .main .block-container {{
                    padding-top: {1}rem;
                }}
            </style>
            ''',unsafe_allow_html=True)

conn = connect(
        user=st.secrets["SNOW_USER"],
        password=st.secrets["SNOW_PASSWORD"],
        account=st.secrets["SNOW_ACCOUNT"],
        role=st.secrets["SNOW_ROLE"],
        warehouse=st.secrets["SNOW_WAREHOUSE"],
)

col1, col2 = st.columns(2)

with col1:

    file = st.file_uploader("Sur excel, les lignes commencent par 1 en comptant les headers, pas les dataframes, il y a donc un écart normal de 2 dans le nombre de lignes")
    if file is not None:
        file_df = pd.read_excel(file, dtype=str)

    table_name= "INPUT_REFERENCES_EXTRACT_OFFERS"
    database= "STAGING_APP"
    schema= "SRCCLIAPP"


    col11, col12, col13 = st.columns(3)

    with col12:
        if st.button('envoyer a snowflake'):
            success, num_chunks, num_rows, output = write_pandas(
                    conn=conn,
                    df=file_df,
                    table_name=table_name,
                    database=database,
                    schema=schema,
                )
            st.write("success =", success)
            st.write("num_chunks =", num_chunks)
            st.write("num_rows =", num_rows)
            st.write("output", output)
        else:
            st.write('')

with col2:
    if file is not None:
        st.dataframe(file_df, use_container_width=True, height= 800)
