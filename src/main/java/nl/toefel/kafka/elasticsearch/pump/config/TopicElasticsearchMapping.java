package nl.toefel.kafka.elasticsearch.pump.config;

import java.util.Optional;

/**
 * @author Christophe Hesters
 */
public class TopicElasticsearchMapping {
    public String topic;
    public String elasticsearchIndex;
    public String elasticsearchType;
    public String elasticsearchIdStrategy;
    public Optional<IdFromFieldInMessageKeyStrategy> idFromField;
    public Boolean configureTimestampInType;

}
