import time
import subprocess

print("===== Comparaison MapReduce vs Hive vs Spark =====")

# =====================
# 1. MapReduce
# =====================
print("\n1) MapReduce")
start = time.time()

subprocess.run([
    "docker", "exec", "namenode",
    "hadoop", "jar",
    "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar",
    "-input", "/datasets/sample_text.txt",
    "-output", "/results/mapreduce_test"
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

time_mapreduce = time.time() - start
print(f"Temps MapReduce : {time_mapreduce:.2f} s")

# =====================
# 2. Hive
# =====================
print("\n2) Hive")
start = time.time()

subprocess.run([
    "docker", "exec", "hive-server",
    "beeline",
    "-u", "jdbc:hive2://localhost:10000",
    "-e", "SELECT COUNT(*) FROM ventes;"
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

time_hive = time.time() - start
print(f"Temps Hive : {time_hive:.2f} s")

# =====================
# 3. Spark
# =====================
print("\n3) Spark")
start = time.time()

subprocess.run([
    "docker", "exec", "spark",
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "/scripts/spark_simple.py"
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

time_spark = time.time() - start
print(f"Temps Spark : {time_spark:.2f} s")

# =====================
# Comparaison
# =====================
print("\n===== Résumé =====")
print(f"MapReduce : {time_mapreduce:.2f} s")
print(f"Hive      : {time_hive:.2f} s")
print(f"Spark     : {time_spark:.2f} s")

fastest = min(
    [("MapReduce", time_mapreduce), ("Hive", time_hive), ("Spark", time_spark)],
    key=lambda x: x[1]
)

print(f"\nLe plus rapide : {fastest[0]}")

