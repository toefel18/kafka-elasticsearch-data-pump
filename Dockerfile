FROM openjdk:9.0.1-11-jre

ADD target/lib modules
ADD target/kafka-elasticsearch-data-pump-1.0-SNAPSHOT.jar modules/kafka-elasticsearch-data-pump.jar

ENV PORT 8080
EXPOSE 8080

CMD exec java -p modules -m nl.toefel.kafka.elasticsearch.pump/nl.toefel.kafka.elasticsearch.pump.Main