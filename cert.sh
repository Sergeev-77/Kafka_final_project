#!/bin/bash
set -e

mkdir -p certs
cd certs

PASSWORD="password"

# ===========================
# 1. Создаём корневой CA (для подписи брокеров)
# ===========================
keytool -genkeypair \
  -alias CARoot \
  -dname "CN=Kafka-CA" \
  -keystore ca.keystore.jks \
  -storepass $PASSWORD \
  -keypass $PASSWORD \
  -validity 3650 \
  -keyalg RSA \
  -keysize 2048 \
  -ext "BC=ca:true,pathlen:0" \
  -noprompt

# Экспортируем публичный сертификат CA
keytool -exportcert \
  -alias CARoot \
  -keystore ca.keystore.jks \
  -storepass $PASSWORD \
  -file kafka-cert.crt \
  -rfc

# ===========================
# 2. Создаём keystore для брокера
# ===========================
keytool -genkeypair \
  -alias kafka \
  -dname "CN=kafka-cluster" \
  -keystore kafka.keystore.jks \
  -storepass $PASSWORD \
  -keypass $PASSWORD \
  -validity 365 \
  -keyalg RSA \
  -keysize 2048 \
  -ext SAN="DNS:kafka-0,DNS:kafka-1,DNS:kafka-mirror-0,DNS:kafka-mirror-1,DNS:localhost,IP:127.0.0.1" \
  -ext "BC=ca:false" \
  -noprompt

# Создаём CSR для брокера
keytool -certreq \
  -alias kafka \
  -keystore kafka.keystore.jks \
  -storepass $PASSWORD \
  -file kafka.csr

# Подписываем CSR с помощью CA
keytool -gencert \
  -alias CARoot \
  -keystore ca.keystore.jks \
  -storepass $PASSWORD \
  -infile kafka.csr \
  -outfile kafka-signed.crt \
  -validity 365 \
  -ext SAN="DNS:kafka-0,DNS:kafka-1,DNS:kafka-mirror-0,DNS:kafka-mirror-1,DNS:localhost,IP:127.0.0.1" \
  -ext "BC=ca:false"

# Импортируем CA в keystore брокера (цепочка сертификатов)
keytool -importcert \
  -alias CARoot \
  -keystore kafka.keystore.jks \
  -storepass $PASSWORD \
  -file kafka-cert.crt \
  -noprompt

# Импортируем подписанный сертификат брокера
keytool -importcert \
  -alias kafka \
  -keystore kafka.keystore.jks \
  -storepass $PASSWORD \
  -file kafka-signed.crt \
  -noprompt

# ===========================
# 3. Создаём truststore для клиентов (Kafka Connect, MM2, HDFS Sink)
# ===========================
keytool -importcert \
  -alias kafka-ca \
  -file kafka-cert.crt \
  -keystore kafka.truststore.jks \
  -storepass $PASSWORD \
  -noprompt

# ===========================
# 4. Очистка временных файлов
# ===========================
rm -f kafka.csr kafka-signed.crt

echo $PASSWORD > password

# ===========================
# 5. Проверка
# ===========================
echo "=== Keystore брокера ==="
keytool -list -keystore kafka.keystore.jks -storepass $PASSWORD

echo "=== Truststore клиентов ==="
keytool -list -keystore kafka.truststore.jks -storepass $PASSWORD
