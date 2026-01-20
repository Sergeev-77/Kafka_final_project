FROM bitnamilegacy/spark:3.5.4

COPY jars/ /opt/bitnami/spark/jars/

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt