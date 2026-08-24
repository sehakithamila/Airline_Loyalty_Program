FROM python:3.10-slim

# Installation de Java pour PySpark
RUN apt-get update && apt-get install -y default-jre wget && rm -rf /var/lib/apt/lists/*

# Téléchargement du driver JDBC dans le dossier système de Python
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/postgresql-42.6.0.jar

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]