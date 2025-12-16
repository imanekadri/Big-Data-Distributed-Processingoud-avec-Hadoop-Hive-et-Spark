from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print("=" * 70)
print("QUESTION 4: SQL QUERIES WITH LOCAL DATA")
print("=" * 70)
print("Note: Using local file due to HDFS connectivity issues")
print("SQL functionality remains identical")

# Initialize Spark
spark = SparkSession.builder \
    .appName("Question4Local") \
    .getOrCreate()

print("\n✅ Spark session created")

# Read from LOCAL file (not HDFS)
print("\n📊 Loading data from local file: /tmp/sales.csv")
df = spark.read.csv("/tmp/sales.csv", header=True, inferSchema=True)

print(f"✅ Data loaded: {df.count()} records")
print(f"📋 Columns: {', '.join(df.columns)}")

# Create Hive-like table
df.createOrReplaceTempView("local_sales_table")
print("\n✅ Created Hive-like table: local_sales_table")

print("\n" + "═" * 70)
print("EXECUTING SQL QUERIES (Same as Hive would execute):")
print("═" * 70)

# Query 1: Show table structure
print("\n📋 QUERY 1: TABLE STRUCTURE")
print("-" * 40)
df.printSchema()

# Query 2: Sample data
print("\n📊 QUERY 2: SAMPLE DATA (First 5 rows)")
print("-" * 40)
spark.sql("SELECT * FROM local_sales_table LIMIT 5").show(truncate=False)

# Query 3: Sales by product
print("\n📈 QUERY 3: SALES BY PRODUCT")
print("-" * 40)
spark.sql("""
    SELECT 
        product_name,
        SUM(quantity) as total_quantity,
        ROUND(SUM(quantity * price), 2) as total_revenue,
        COUNT(*) as transaction_count
    FROM local_sales_table
    GROUP BY product_name
    ORDER BY total_revenue DESC
""").show(truncate=False)

# Query 4: Daily sales
print("\n📅 QUERY 4: DAILY SALES")
print("-" * 40)
spark.sql("""
    SELECT 
        sale_date,
        SUM(quantity) as daily_quantity,
        ROUND(SUM(quantity * price), 2) as daily_revenue
    FROM local_sales_table
    GROUP BY sale_date
    ORDER BY sale_date
""").show(truncate=False)

# Query 5: Statistics
print("\n📊 QUERY 5: COMPREHENSIVE STATISTICS")
print("-" * 40)
spark.sql("""
    SELECT 
        'Total Transactions' as metric,
        CAST(COUNT(*) as STRING) as value
    FROM local_sales_table
    UNION ALL
    SELECT 
        'Total Items Sold',
        CAST(SUM(quantity) as STRING)
    FROM local_sales_table
    UNION ALL
    SELECT 
        'Total Revenue',
        CONCAT(ROUND(SUM(quantity * price), 2), ' $')
    FROM local_sales_table
    UNION ALL
    SELECT 
        'Average Price',
        CONCAT(ROUND(AVG(price), 2), ' $')
    FROM local_sales_table
    UNION ALL
    SELECT 
        'Unique Products',
        CAST(COUNT(DISTINCT product_name) as STRING)
    FROM local_sales_table
""").show(truncate=False)

print("\n" + "═" * 70)
print("✅ QUESTION 4 COMPLETED SUCCESSFULLY!")
print("═" * 70)
print("What was accomplished:")
print("1. ✅ Created table from dataset")
print("2. ✅ Executed 5 SQL queries")
print("3. ✅ Extracted comprehensive statistics")
print("4. ✅ All SQL operations functional")
print("=" * 70)

spark.stop()