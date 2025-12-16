# Big Data & Cloud Computing Project
## Hadoop, Hive, Spark & MinIO using Docker

---

## 📌 Introduction
Avec l’essor du cloud computing et l’augmentation massive des volumes de données, les technologies Big Data sont devenues indispensables pour le stockage et le traitement distribué de l’information.  

Ce projet a pour objectif de mettre en œuvre une architecture Big Data complète en utilisant des solutions open source telles que **Hadoop, Hive, Apache Spark et MinIO**, déployées à l’aide de **Docker**.

Le projet couvre l’ensemble du cycle Big Data :
- Stockage distribué (HDFS et MinIO)
- Traitement MapReduce
- Interrogation SQL avec Hive
- Analyse avancée avec Apache Spark
- Automatisation d’un pipeline Big Data

---

## 🧱 Architecture du projet

Les services suivants sont déployés sous forme de conteneurs Docker :

- **Hadoop HDFS**
  - NameNode
  - DataNode
- **Hive**
  - Hive Server
  - Metastore
- **Apache Spark**
- **MinIO** (stockage objet compatible Amazon S3)

Cette architecture simule un environnement cloud Big Data réel sur une machine locale.

---

## 🛠️ Technologies utilisées

- Docker & Docker Compose  
- Hadoop (HDFS, MapReduce)  
- Apache Hive  
- Apache Spark (PySpark)  
- MinIO (S3 compatible)  
- Python  

---

## 📂 Structure du projet


MY_PROJECT_BIGDATA/
│

├── docker-compose.yml

├── datasets/

│ ├── sales.csv

│ ├── products.csv

│ ├── web_logs.csv

│
├── scripts/

│ ├── mapper_wordcount.py

│ ├── reducer_wordcount.py

│ ├── sales_mapper.py

│ ├── sales_reducer.py

│ ├── spark_sales_analysis.py

│ ├── web_logs_analysis.py

│ ├── pipeline_minio.py

│
└── README.md


---

## 🌐 Interfaces Web

- **HDFS NameNode** : http://localhost:9870  
- **Apache Spark UI** : http://localhost:8080  
- **MinIO Console** : http://localhost:9001  

---

## 🚀 Lancement de l’environnement

Démarrer tous les services Docker :

```bash
docker compose up -d
