# Kafka -> Elasticsearch data pump

Service that takes incoming streams from kafka topics and writes them
to elasticsearch in a configurable way.

Quickstart:

    docker pull toefel/kafka-elasticsearch-data-pump
    docker run -d --name kafka-elasticsearch-data-pump -p 8080:8080 toefel/kafka-elasticsearch-data-pump
    
    #Then view the logs to see if it's running.
    
    docker logs kafka-elasticsearch-data-pump
    
    #Then configure it via REST
    curl -XPUT -d @example-config.json localhost:8080/configuration
    
Your messages should now end up in ELK. if Elasticsearch requires authentication, that is not yet supported


#Development

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
    