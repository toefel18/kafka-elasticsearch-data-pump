package nl.toefel.kafka.elasticsearch.pump;

import jdk.incubator.http.HttpClient;
import nl.toefel.kafka.elasticsearch.pump.config.Config;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * @author Christophe Hesters
 */
public class ConfigurableStreams {

    private final HttpClient client = HttpClient.newHttpClient();
    private KafkaStreams activeStreams;

    public void reconfigureStreams(Config config) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-elasticsearch-data-pump-");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        config.topicMappings.forEach(mapping -> {
            KStream<String, String> stream = builder.stream(mapping.topic);
            if ("random".equalsIgnoreCase(mapping.elasticsearchIdStrategy)) {
                stream.foreach((key, val) -> System.out.println("received " + key + " => " + val));
                //TODO continue here
            }
        });

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

}
