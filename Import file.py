import pandas as pd
import streamlit as st
import streamlit_nested_layout
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from st_pages import Page, show_pages, add_page_title

st.set_page_config(
    page_title="Import file",
    layout="wide"
)

st.markdown("""
        <style>
               .block-container {
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 5rem;
                    padding-right: 5rem;
                }
        </style>
        """, unsafe_allow_html=True)

add_page_title()

conn = connect(
        user=st.secrets["SNOW_USER"],
        password=st.secrets["SNOW_PASSWORD"],
        account=st.secrets["SNOW_ACCOUNT"],
        role=st.secrets["SNOW_ROLE"],
        warehouse=st.secrets["SNOW_WAREHOUSE"],
)

col1, col2 = st.columns(2)

with col1:

    file = st.file_uploader("")
    if file is not None:
        file_df = pd.read_excel(file, dtype=str)

    table_name= "INPUT_REFERENCES_EXTRACT_OFFERS"
    database= "STAGING_APP"
    schema= "SRCCLIAPP"


    col11, col12, col13 = st.columns(3)

    with col12:
        if st.button('Send to snowflake'):
            success, num_chunks, num_rows, output = write_pandas(
                    conn=conn,
                    df=file_df,
                    table_name=table_name,
                    database=database,
                    schema=schema,
                    # snowflake error when overwrite is True (missing privileges ?)
                    # overwrite=True
                )
            st.write("success =", success)
            st.write("num_chunks =", num_chunks)
            st.write("num_rows =", num_rows)
            st.write("output", output)
        else:
            st.write('')

with col2:
    if file is not None:
        st.dataframe(file_df, use_container_width=True, height= 700)
#
