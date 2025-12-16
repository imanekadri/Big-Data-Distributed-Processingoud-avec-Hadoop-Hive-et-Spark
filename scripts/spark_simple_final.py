from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

spark = SparkSession.builder \
    .appName("Spark_MinIO_Sales") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

#  Lire les datasets depuis MinIO
df_sales = spark.read.csv("s3a://datasets/sales.csv", header=True, inferSchema=True)
df_products = spark.read.csv("s3a://datasets/products.csv", header=True, inferSchema=True)

#  Filter
df_filter = df_sales.filter(col("quantity") > 5)

#  GroupBy + Aggregation
df_group = df_sales.groupBy("product_name").agg(
    count("*").alias("nombre_ventes"),
    sum("quantity").alias("quantite_totale"),
    avg("price").alias("prix_moyen")
)

#  Join
df_join = df_sales.join(df_products, "product_id", "inner")

#  Sauvegarde sur MinIO
df_group.write.mode("overwrite").csv("s3a://datasets/results/sales_by_product", header=True)
df_join.write.mode("overwrite").csv("s3a://datasets/results/sales_join", header=True)
