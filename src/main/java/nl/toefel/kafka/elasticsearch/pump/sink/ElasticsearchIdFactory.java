package nl.toefel.kafka.elasticsearch.pump.sink;

import nl.toefel.kafka.elasticsearch.pump.config.TopicElasticsearchMapping;

public class ElasticsearchIdFactory {
    public ElasticsearchRecordIdStrategy getStrategy(TopicElasticsearchMapping mapping) {
        if (mapping.elasticsearchIdStrategy == null || mapping.elasticsearchIdStrategy.isEmpty()) {
            System.out.println("elasticsearchIdStrategy is null for " + mapping.topic + ", defaulting to auto");
            mapping.elasticsearchIdStrategy = "auto";
        }
        switch (mapping.elasticsearchIdStrategy) {
            case "random":
                return new RandomIdStrategy();
            case "fromField":
                return new FromFieldIdStrategy(mapping.pathToIdInMessage.orElseThrow(() -> new IllegalStateException("fromField requires pathToIdInMessage to be a JSON path to the ID in Kafka")));
            case "fromKafkaKey":
                return new FromKafkaKeyIdStrategy();
            case "auto":
                return new AutoIdStrategy();
            default:
                throw new IllegalStateException("unknown ID strategy " + mapping.elasticsearchIdStrategy + ", use auto, random, fromField or fromKafkaKey");
        }
    }
}
