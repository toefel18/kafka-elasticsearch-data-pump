package nl.toefel.kafka.elasticsearch.pump.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Christophe Hesters
 */
public class Config {
    public static final Path CONFIG_PATH = Paths.get(System.getProperty("user.home"), ".kafka-elasticsearch-data-pump-config.json");

    public String kafkaConsumerGroupId;
    public String kafkaBootstrapServers;
    public String elasticsearchServer;
    public String kafkaAutoOffsetReset;
    public List<TopicElasticsearchMapping> topicMappings;

    public static Config newEmpty() {
        return new Config();
    }

    @JsonIgnore
    public boolean isEmpty() {
        List<Object> fields = Arrays.asList(kafkaBootstrapServers, kafkaConsumerGroupId, elasticsearchServer, topicMappings);
        return fields.stream().allMatch(Objects::isNull);
    }

    @JsonIgnore
    public boolean isValid() {
        return valid("elasticsearchServer", elasticsearchServer)
                && valid("kafkaConsumerGroupId", kafkaConsumerGroupId)
                && valid("kafkaBootstrapServers", kafkaBootstrapServers)
                && valid("topicMappings", topicMappings);
    }

    @JsonIgnore
    public static List<String> requiredFieldsToBeValid() {
        return Arrays.asList("elasticsearchServer", "kafkaConsumerGroupId", "kafkaBootstrapServers", "topicMappings");
    }

    private static boolean valid(String name, Object actual) {
        boolean emptyString = actual instanceof String && ((String) actual).isEmpty();
        boolean emptyCollection = actual instanceof Collection && ((Collection) actual).isEmpty();
        if (actual == null || emptyString || emptyCollection) {
            System.out.println(name + " is emtpy or not set");
            return false;
        }
        return true;
    }
}
