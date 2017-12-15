package nl.toefel.kafka.elasticsearch.pump;

import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.config.TopicElasticsearchMapping;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Christophe Hesters
 */
public class Main {

    public static final Path cfgPath = Paths.get(System.getProperty("user.home"), ".kafka-elasticsearch-data-pump", "config.json");

    public static void main(String[] args) throws IOException {
        Config cfg = Config.newEmpty();
        if (Files.exists(cfgPath)) {
            cfg = Jsonizer.fromJson(Files.readAllBytes(cfgPath), Config.class);
        }
        printConfig(cfg);
        ConfigurableStreams streams = new ConfigurableStreams();
        streams.reconfigureStreams(cfg);
    }

    private static void printConfig(Config cfg) {
        System.out.println("Using config: \n" + Jsonizer.toJsonFormatted(cfg));
    }
}
