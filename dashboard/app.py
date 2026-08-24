import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres:5432/loyalty_db")
engine = create_engine(DB_URL)

st.set_page_config(page_title="Dashboard Fidélité", layout="wide")
st.title("📊 Impact de la Campagne de Fidélité")

st.header("1. Impact sur les inscriptions (Brutes / Nettes)")
df1 = pd.read_sql("SELECT * FROM insight_1", engine)
st.dataframe(df1, use_container_width=True)

st.header("2. Adoption selon le profil démographique")
df2 = pd.read_sql("SELECT * FROM insight_2", engine)
st.bar_chart(df2.pivot(index="education", columns="gender", values="membres_promo_2018"))

st.header("3. Impact sur les vols réservés en été")
df3 = pd.read_sql("SELECT * FROM insight_3", engine)
st.dataframe(df3, use_container_width=True)