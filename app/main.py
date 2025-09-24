import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from kafka import KafkaProducer

# Wait for services to be ready
import time
time.sleep(20) # We need to wait a little longer for Spark to start and connect to services

# ========================
# 1️⃣ Créer une session Spark pour se connecter à PostgreSQL
# ========================
spark = (
    SparkSession.builder
    .appName("PySpark-Postgres-Kafka-App")
    .config("spark.jars", "postgresql-42.7.3.jar") # Nom du jar dans le conteneur
    .getOrCreate()
)

print("✅ Spark Session créée avec succès.")

# ========================
# 2️⃣ Générer des données de test
# ========================
schema = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True)
])

data = [
    (1, "test1@email.com", "John Doe"),
    (2, "test2@email.com", "Jane Smith"),
    (3, "test3@email.com", "Peter Jones")
]

df_spark = spark.createDataFrame(data, schema)
df_spark.show()

# ========================
# 3️⃣ Paramètres de connexion PostgreSQL
# ========================
jdbc_url = "jdbc:postgresql://postgres:5432/mydatabase" # Utilise le nom du service "postgres"
table_name = "clients"
properties = {
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# ========================
# 4️⃣ Insérer les données dans PostgreSQL
# ========================
df_spark.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)
print("✅ Données insérées dans la table PostgreSQL :", table_name)

# ========================
# 5️⃣ Envoyer les données à Kafka
# ========================
try:
    producer = KafkaProducer(bootstrap_servers=os.environ.get("KAFKA_BROKERS").split(','))
    for row in df_spark.collect():
        producer.send('client-topic', str(row.asDict()).encode('utf-8'))
    producer.flush()
    print("✅ Données envoyées à Kafka.")
except Exception as e:
    print(f"Erreur lors de l'envoi des données à Kafka : {e}")

# ========================
# 6️⃣ Lire la table pour vérifier
# ========================
df_postgres = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
df_postgres.show()

# ========================
# 7️⃣ Fermer Spark
# ========================
spark.stop()