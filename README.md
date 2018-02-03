# Kafka -> Elasticsearch data pump

Service that takes incoming streams from kafka topics and writes them
to elasticsearch in a configurable way.

## Quickstart using Docker (local one node cluster)
    
TODO provide a docker-compose file that spins this all up.    
    
 1. Get your IP address 
  
        hostname -I  
        
 1. Start a single elasticsearch container  
    
        docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.1.2
   
 1. Kibana will try to find elasticsearch on  elasticsearch.url http://elasticsearch:9200, so the --link accomplishes that
    
        docker run -d --name kibana --link elasticsearch:elasticsearch -p 5601:5601 docker.elastic.co/kibana/kibana:6.1.2
    
 1. Start a development kafka
 
        docker run -d --name kafka -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=<YOUR MACHINE IP ADDRESS> --env ADVERTISED_PORT=9092 spotify/kafka
    
 1. Start the data pump
        
        docker run -d --name kafka-elasticsearch-data-pump -p 8080:8080 toefel/kafka-elasticsearch-data-pump:latest
 
 1. Configure the data pump
 
    Create a configuration file similar to this and save it as `~/data-pump-config.json`:
 
        {
          "kafkaConsumerGroupId" : "kafka-elasticsearch-data-pump-1",
          "kafkaBootstrapServers" : "<YOUR MACHINE IP ADDRESS>:9092",
          "elasticsearchServer" : "<YOUR MACHINE IP ADDRESS>:9200",
          "topicMappings" : [ {
            "topic" : "any-topic-you-like",
            "elasticsearchIndex" : "kafka-topics-data-pump-tester",
            "elasticsearchType" : "data-pump-tester",
            "elasticsearchIdStrategy" : "auto",
            "addKafkaMetaData" : true,
            "configureTimestampInType" : true,
            "logCurlCommands" : "always"
          }, {
            "topic" : "flow-unit-status",
            "elasticsearchIndex" : "kafka-topics-flow-unit-status",
            "elasticsearchType" : "flow-unit-status",
            "elasticsearchIdStrategy" : "auto",
            "addKafkaMetaData" : true,
            "configureTimestampInType" : true,
            "logCurlCommands" : "always"
          }]
        }
    
    Configure the service to use that config:
    
        curl -XPUT -d @~/data-pump-config.json http://localhost:8080/configuration
        
    This configuration is persisted within the container, a restart of the container will immediately start with this 
    config. 
 
Your messages should now end up in Elasticsearch, to view them with Kibana:

 1. Open [kibana](kibana at http://localhost:8080)
 1. Goto Management > Index patterns > Create Index Pattern
 1. Enter `kafka-topics-*` as the pattern and click *Next Step*
 1. Choose `timestamp` as the Time filter field name and click *Create index pattern*
 1. Goto Discover and click on the time clock at the top right (probably saying `Last 15 minutes`)
 1. Select `Last year`
 1. You should now see all your kafka messages 
  
**Authentication on elasticsearch is not yet supported**

#Development

Requires maven 3.5 and Java 9.

To start this service: 

1. place a valid config in `<user-home>/.kafka-elasticsearch-data-pump/config.json`, see [example-config.json](example-config.json).
   OR use the REST api to load a config via `curl -XPUT -d @config.json localhost:8080/configuration` 
2. run Main

## REST API

the service exposes two resources:

 * /configuration 
    - GET retrieves the currently active and possible error information. [Config](src/main/java/nl/toefel/kafka/elasticsearch/pump/config/Config.java)
    - PUT activates a new [Config](src/main/java/nl/toefel/kafka/elasticsearch/pump/config/Config.java) and restarts the streaming application. This config is persisted in `<HOME>/.kafka-elasticsearch-data-pump/config.json` and automatically reloaded on startup. 
 * /statistics
    - GET retrieves the collected internal statistics
    - DELETE retrieves the collected internal statistics and resets them afterwards
    
### Configuration options    
      
        {
          "kafkaConsumerGroupId" : "kafka-elasticsearch-data-pump-1",  // the kafka-consumer-group to use
          "kafkaBootstrapServers" : "<YOUR MACHINE IP ADDRESS>:9092",  // kafka address, comma separated list host:port 
          "elasticsearchServer" : "<YOUR MACHINE IP ADDRESS>:9200",    // an elasticsearch server address, single item!
          "topicMappings" : [ {                                        // list of per-topic configurations
            "topic" : "any-topic-you-like",                            // the name of the topic to consume from
            "elasticsearchIndex" : "kafka-topics-data-pump-tester",    // the elasticsearch index to put messages in (created automatically if not exists)
            "elasticsearchType" : "data-pump-tester",                  // the elasticsearch type to use/create (created automatically if not exists)
            "elasticsearchIdStrategy" : "auto",                        // Options are "auto": let elasticsearch determine the ID of each message  
                                                                       //             "fromKafkaKey": use the key in the kafka message as ID (know what you are doing!)
                                                                       //             "fromField": (not yet implemented, but would require a jsonpath to the field in the message)
                                                                       //             "random": generate a random UUID.
                                                                       //             the default is auto 
            "addKafkaMetaData" : true,                                 // Add all the information from a ConsumerRecord like topic, partition, offset, message size, etc
                                                                       // to the message posted to elasticsearch with the field _kafkaMetaData (so that you can use this in aggregations)  
            "configureTimestampInType" : true,                         // Configure elasticsearch type to use timestamp as the time identifier
                                                                       // The data pump harmonizes all timestamp fields to make sure you can use the time filter in kibana.
            "logCurlCommands" : "always"                               // always: logs each call to elasticsearch as a curl command, allow manual retries
                                                                       // onFailure: log failed calls to elasticsearch as a curl command, to allow manual fixing
          }, {
            "topic" : "flow-unit-status",
            "elasticsearchIndex" : "kafka-topics-flow-unit-status",
            "elasticsearchType" : "flow-unit-status",
            "elasticsearchIdStrategy" : "auto",
            "addKafkaMetaData" : true,
            "configureTimestampInType" : true,
            "logCurlCommands" : "always"
          }]
        }
    
## Docker

#### Building an image

    mvn clean install
    docker build -t toefel/kafka-elasticsearch-data-pump:latest .
    docker push
    
#### Running the image

    docker run -d -p 8080:8080 --name kafka-elasticsearch-data-pump toefel/kafka-elasticsearch-data-pump:latest
   