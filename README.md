# Kafka -> Elasticsearch data pump

Service that takes incoming streams from kafka topics and writes them
to elasticsearch in a configurable way.

Requires maven 3.5 and Java 9.

To start this service: 

1. place a valid config in `<user-home>/.kafka-elasticsearch-data-pump/config.json`, see [example-config.json](example-config.json).
   OR use the REST api to load a config via `curl -XPUT -d @config.json localhost:8080/configuration` 
2. run Main

## REST API

the service exposes two resources:

 * /configuration 
    - GET retrieves the currently active [Config](src/main/java/nl/toefel/kafka/elasticsearch/pump/config/Config.java)
    - PUT activates a new [Config](src/main/java/nl/toefel/kafka/elasticsearch/pump/config/Config.java) and restarts the streaming application. This config is persisted in `<HOME>/.kafka-elasticsearch-data-pump/config.json` and automatically reloaded on startup. 
 * /topology
    - GET retrieves the currently active Kafka Streams Topology. 
    
## Docker

#### Building an image

    docker build -t toefel/kafka-elasticsearch-data-pump:v1 .
    docker push
    
#### Running the image

    docker run -d -p 8080:8080 --name kafka-elasticsearch-data-pump toefel/kafka-elasticsearch-data-pump:v1
    