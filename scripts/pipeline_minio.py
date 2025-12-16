from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

# ===== Initialisation Spark avec MinIO =====
spark = SparkSession.builder \
    .appName("Pipeline_MinIO") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ===== Lecture des datasets depuis MinIO =====
df_sales = spark.read.csv("s3a://datasets/sales.csv", header=True, inferSchema=True)
df_products = spark.read.csv("s3a://datasets/products.csv", header=True, inferSchema=True)
df_logs = spark.read.csv("s3a://datasets/web_logs.csv", header=True, inferSchema=True)

# ===== Transformations et analyses =====
# Filter ventes
df_filter = df_sales.filter(col("quantity") > 5)

# GroupBy ventes par produit
df_group = df_sales.groupBy("product_name").agg(
    count("*").alias("nombre_ventes"),
    sum("quantity").alias("quantite_totale"),
    avg("price").alias("prix_moyen")
)

# Jointure ventes + produits
df_join = df_sales.join(df_products, "product_id", "inner")

# Analyse logs web
total_views = df_logs.count()
unique_users = df_logs.select("user_id").distinct().count()
df_top_pages = df_logs.groupBy("page").count().orderBy("count", ascending=False)

# ===== Sauvegarde sur MinIO =====
df_group.write.mode("overwrite").csv("s3a://datasets/results/sales_by_product", header=True)
df_join.write.mode("overwrite").csv("s3a://datasets/results/sales_join", header=True)
df_top_pages.write.mode("overwrite").csv("s3a://datasets/results/web_top_pages", header=True)

print(f"✅ Pipeline terminé. Résultats sauvegardés sur MinIO dans 'datasets/results/'")

spark.stop()

