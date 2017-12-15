package nl.toefel.kafka.elasticsearch.pump.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Christophe Hesters
 */
public class Config {
    public String kafkaConsumerGroupId;
    public String kafkaBootstrapServers;
    public String elasticsearchServer;
    public List<TopicElasticsearchMapping> topicMappings;

    public static Config newEmpty() {
        Config cfg = new Config();
        cfg.topicMappings = new ArrayList<>();
        return cfg;
    }

    @JsonIgnore
    public boolean isValid() {
        return valid("elasticsearchServer", elasticsearchServer)
                && valid("kafkaConsumerGroupId", kafkaConsumerGroupId)
                && valid("kafkaBootstrapServers", kafkaBootstrapServers)
                && valid("topicMappings", topicMappings);
    }

    private static final boolean valid(String name, Object actual) {
        boolean emptyString = actual instanceof String && ((String) actual).isEmpty();
        boolean emptyCollection = actual instanceof Collection && ((Collection) actual).isEmpty();
        if (actual == null || emptyString || emptyCollection) {
            System.out.println(name + " is emtpy or not set");
            return false;
        }
        return true;
    }
}
