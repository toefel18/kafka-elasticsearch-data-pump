# Kafka -> Elasticsearch data pump

Service that takes incoming streams from kafka topics and writes them
to elasticsearch in a configurable way.

Requires maven 3.5 and Java 9.

To start this service: 

1. place a valid config in `<user-home>/.kafka-elasticsearch-data-pump/config.json`, see [example-config.json](example-config.json). 
2. run Main

Next steps:

Package in docker and allow configuration via REST. Config should persist if container restarts. 