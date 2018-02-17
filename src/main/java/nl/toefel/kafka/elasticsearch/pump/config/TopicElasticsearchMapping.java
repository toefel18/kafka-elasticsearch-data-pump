package nl.toefel.kafka.elasticsearch.pump.config;

import java.util.Objects;
import java.util.Optional;

/**
 * @author Christophe Hesters
 */
public class TopicElasticsearchMapping {
    public String topic;
    public String elasticsearchIndex;
    public String elasticsearchType;
    public String elasticsearchIdStrategy;
    public Boolean addKafkaMetaData;
    public Optional<String> pathToIdInMessage;
    public Boolean configureTimestampInType;
    public String logCurlCommands;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicElasticsearchMapping that = (TopicElasticsearchMapping) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(elasticsearchIndex, that.elasticsearchIndex) &&
                Objects.equals(elasticsearchType, that.elasticsearchType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, elasticsearchIndex, elasticsearchType);
    }
}
