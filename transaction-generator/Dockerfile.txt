# Utiliser une image Python légère comme base
FROM python:3.10-slim

# Définir le répertoire de travail
WORKDIR /app

# Installer une version de JRE (Java Runtime Environment) nécessaire pour PySpark et le driver JDBC
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/*

# Copier le fichier de dépendances et les installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Télécharger le driver JDBC de PostgreSQL en utilisant ADD, qui est plus fiable que wget
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar .

# Copier le script PySpark
COPY main.py .

# Définir la commande d'exécution
CMD ["python", "main.py"]
