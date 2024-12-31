"""Streamlit app."""

import duckdb
import streamlit as st

from dotenv import load_dotenv

DATABASE = "FG_DWH"

load_dotenv()

con = duckdb.connect(database=f"md:{DATABASE}", read_only=True)

st.title("Green Grid Geeks")

query = """
SELECT
    Month as month,
    SUM(WindPowerGenerated) AS megawatts
FROM mockdashstaging.windpower2023staging
GROUP BY Month
ORDER BY Month;
"""
df = con.execute(query).df()

st.subheader("Wind Power Generated")
st.line_chart(df, x="month")
