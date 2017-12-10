package nl.toefel.kafka.elasticsearch.pump.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author Christophe Hesters
 */
public class Config {
    public List<TopicElasticsearchMapping> topicMappings;
}
