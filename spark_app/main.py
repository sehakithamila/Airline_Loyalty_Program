import os
import glob
import re
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/spark-data"
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
DB_URL_JDBC = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/loyalty_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Connection SQLAlchemy pour la persistance brute
db_engine_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@postgres:5432/loyalty_db"
engine = create_engine(db_engine_url)

def clean_col_name(col):
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(col).strip().lower())

print("--> 1. Ingestion des CSV et persistance brute dans PostgreSQL...")

csv_files = glob.glob(f"{DATA_DIR}/*.csv")
for file_path in csv_files:
    raw_name = os.path.basename(file_path).replace('.csv', '')
    table_name = f"raw_{clean_col_name(raw_name)}"
    
    # Lecture Pandas + écriture directe dans Postgres
    df_raw = pd.read_csv(file_path)
    df_raw.columns = [clean_col_name(c) for c in df_raw.columns]
    df_raw.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"    Table créée : {table_name}")

print("--> 2. Connexion PySpark au Master Spark (spark://spark-master:7077)...")

spark = SparkSession.builder \
    .appName("AirlineLoyaltyPipeline") \
    .master(SPARK_MASTER) \
    .config("spark.jars", "/opt/postgresql-42.6.0.jar") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

print("--> 3. Calcul des Insights métier avec Spark...")

# Lecture des tables brutes depuis Postgres via JDBC
df_loyalty = spark.read.format("jdbc") \
    .option("url", DB_URL_JDBC).option("dbtable", "raw_customer_loyalty_history") \
    .option("user", DB_USER).option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver").load()

df_flight = spark.read.format("jdbc") \
    .option("url", DB_URL_JDBC).option("dbtable", "raw_customer_flight_activity") \
    .option("user", DB_USER).option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver").load()

# Insight 1: Inscriptions Brutes vs Nettes
df_insight1 = df_loyalty.groupBy("enrollment_type") \
    .agg(
        F.count("loyalty_number").alias("inscriptions_brutes"),
        F.sum(F.when(F.col("cancellation_year").isNotNull(), 1).otherwise(0)).alias("annulations"),
        (F.count("loyalty_number") - F.sum(F.when(F.col("cancellation_year").isNotNull(), 1).otherwise(0))).alias("inscriptions_nettes")
    )

# Insight 2: Profils démographiques
df_insight2 = df_loyalty.groupBy("gender", "education") \
    .agg(
        F.count("*").alias("total_membres"),
        F.sum(F.when(F.col("enrollment_type") == "2018 Promotion", 1).otherwise(0)).alias("membres_promo_2018")
    )

# Insight 3: Vols d'été (Mois 6, 7, 8)
df_loyalty_promo = df_loyalty.select("loyalty_number", "enrollment_type")
df_summer_flights = df_flight.filter(F.col("month").isin([6, 7, 8])) \
    .join(df_loyalty_promo, "loyalty_number", "inner") \
    .groupBy("enrollment_type", "year") \
    .agg(F.sum("total_flights").alias("vols_ete_totaux"))

# Sauvegarde des résultats transformés
outputs = [
    (df_insight1, "insight_1"),
    (df_insight2, "insight_2"),
    (df_summer_flights, "insight_3")
]

for df_res, table in outputs:
    df_res.write \
        .format("jdbc") \
        .option("url", DB_URL_JDBC) \
        .option("dbtable", table) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

print("--> Pipeline terminé avec succès !")
spark.stop()