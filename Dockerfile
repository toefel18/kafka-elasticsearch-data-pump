FROM openjdk:9.0.1-11-jre

ADD target/lib modules
ADD target/kafka-elasticsearch-data-pump-1.0-SNAPSHOT.jar modules/kafka-elasticsearch-data-pump.jar

CMD exec java -p modules -m nl.toefel.kafka.elasticsearch.pump/nl.toefekafka.elasticsearch.pump.Main