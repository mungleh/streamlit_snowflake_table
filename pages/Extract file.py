import pandas as pd
import streamlit as st
from snowflake.connector import connect
import io
from st_pages import Page, show_pages, add_page_title

st.set_page_config(
    page_title="Extract file",
    layout="wide"
)

add_page_title()

buffer = io.BytesIO()

conn = connect(
        user=st.secrets["SNOW_USER"],
        password=st.secrets["SNOW_PASSWORD"],
        account=st.secrets["SNOW_ACCOUNT"],
        role=st.secrets["SNOW_ROLE"],
        warehouse=st.secrets["SNOW_WAREHOUSE"],
        database=st.secrets["SNOW_DATABASE"],
        schema=st.secrets["SNOW_SCHEMA"]
)

preview = pd.read_sql('SELECT * from OFFERS_FROM_INPUTS_V LIMIT 100',conn)
st.dataframe(preview, use_container_width=True, height= 500)

view = pd.read_sql('SELECT * from OFFERS_FROM_INPUTS_V',conn)
df = pd.DataFrame(view)
count_row = df.shape[0]
st.write("file has", count_row, "rows")

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(sep = ';', index=False).encode('utf-8')
csv = convert_df(df)

col1, col2, col3, col4 = st.columns(4)

with col1:
    if count_row < 1048576:
        st.session_state.get("disabled", True)
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            # Write each dataframe to a different worksheet.
            df.to_excel(writer, index=False)

            writer.close()

            download_excel = st.download_button(
                label="Download as Excel",
                data=buffer,
                file_name="SNOWFLAKE_OFFERS.xlsx",
                mime="application/vnd.ms-excel"
            )
    else:
        st.write("Number of rows exceed excels limit")

with col2:
        download_csv = st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='SNOWFLAKE_OFFERS.csv',
            mime='text/csv',
        )
