FROM confluentinc/cp-kafka-connect:7.7.1

USER root

# Скопировать JMX Exporter в докер образ
COPY jmx_prometheus_javaagent-0.15.0.jar /opt/
# Скопировать настройки JMX Exporter в докер образ
COPY kafka-connect.yml /opt/

RUN mkdir -p /usr/share/java/kafka-mirror-maker && \
    curl -L https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz \
    | tar -xz --strip-components=1 -C /usr/share/java/kafka-mirror-maker \
    kafka_2.13-3.7.0/libs

USER appuser