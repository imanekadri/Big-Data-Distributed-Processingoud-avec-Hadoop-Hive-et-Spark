

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, desc

# INITIALISATION SPARK + MinIO

spark = SparkSession.builder \
    .appName("WebLogsAnalysis") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


 
# CHARGEMENT DU DATASET LOGS WEB DEPUIS MinIO

minio_path = "s3a://datasets/web_logs.csv" 
df_logs = spark.read.csv(minio_path, header=True, inferSchema=True)

print("📄 Aperçu du dataset Web Logs:")
df_logs.show(truncate=False)


# STATISTIQUES DE BASE

# Nombre total de pages vues
total_views = df_logs.count()

# Nombre d'utilisateurs uniques
unique_users = df_logs.select(countDistinct("user_id")).collect()[0][0]

print(f"Total page views: {total_views}")
print(f"Unique users: {unique_users}")


# TOP PAGES

top_pages = df_logs.groupBy("page") \
    .agg(count("*").alias("views")) \
    .orderBy(desc("views"))


top_pages.show(truncate=False)

# SAUVEGARDE DES RÉSULTATS SUR MinIO

df_logs.write.mode("overwrite").csv("s3a://datasets/results/web_logs", header=True)
top_pages.write.mode("overwrite").csv("s3a://datasets/results/top_pages", header=True)

print(" Résultats sauvegardés sur MinIO dans datasets/results/")

spark.stop()




