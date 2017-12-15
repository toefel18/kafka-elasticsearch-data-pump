package nl.toefel.kafka.elasticsearch.pump;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.config.TopicElasticsearchMapping;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import nl.toefel.patan.StatisticsFactory;
import nl.toefel.patan.api.Statistics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author Christophe Hesters
 */
public class ConfigurableStreams {

    private static final String TYPE_CONFIG = "{\"mappings\":{\"${TYPE}\":{\"properties\":{\"timestamp\":{\"type\":\"date\"}}}}}";

    private final Statistics stats = StatisticsFactory.createThreadsafeStatistics();
    private final HttpClient http = HttpClient.newHttpClient();
    private KafkaStreams activeStreams;

    public void reconfigureStreams(Config config) {
        if (!config.isValid()) {
            System.out.println("Not altering config due to invalid parameters");
            return;
        }
        if (activeStreams != null) {
            System.out.println("Closing active streams");
            activeStreams.close();
        }
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.kafkaConsumerGroupId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        config.topicMappings.forEach(mapping -> {
            preprocess(mapping, config);
            builder.stream(mapping.topic)
                    .filter(this::validJsonValues)
                    .map(this::insertTimestamp)
                    .foreach((key, val) -> onRecord(String.valueOf(key), val, mapping, config));
        });

        Topology topology = builder.build();
        System.out.println(topology.describe());

        activeStreams = new KafkaStreams(topology, props);
        activeStreams.start();
    }

    private void preprocess(TopicElasticsearchMapping mapping, Config config) {
        if (mapping.configureTimestampInType == null || mapping.configureTimestampInType) {
            String uri = "http://" + Paths.get(config.elasticsearchServer, mapping.elasticsearchIndex, mapping.elasticsearchType);
            String body = TYPE_CONFIG.replace("${TYPE}", mapping.elasticsearchType);
            HttpRequest request = HttpRequest.newBuilder(URI.create(uri))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyProcessor.fromString(body))
                    .build();
            try {
                System.out.println("curl -XPUT -H 'Content-Type: application/json' " + uri + " -d '" + body + "'");
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandler.asString());
                System.out.println("Status: " + response.statusCode() + ", body: " + response.body());
            } catch (IOException | InterruptedException e) {
                System.out.println("Failed to send type configuration request to " + uri + ", with body: " + body);
            }
        }
    }

    //TODO optimize double json parsing
    private boolean validJsonValues(Object key, Object value) {
        try {
            Jsonizer.fromJson(String.valueOf(value));
            return true;
        } catch (Exception e) {
            System.out.println("Skipping message with invalid JSON key: " + String.valueOf(key) + ", value: " + String.valueOf(value));
            return false;
        }
    }

    private KeyValue<Object, String> insertTimestamp(Object key, Object val) {
        Map<String, Object> parsedJson = Jsonizer.fromJson(String.valueOf(val));
        Object timestamp = parsedJson.get("timestamp");
        if (timestamp != null) {
            parsedJson.put("originalTimestampInMessage", timestamp);
        }
        if (timestamp instanceof Long) {
            parsedJson.put("timestamp", ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long)timestamp), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        } else if (timestamp instanceof String) {
            // nothing ot do
        } else {
            System.out.println("INCOMPATIBLE TIMESTAMP FORMAT " + timestamp);
            parsedJson.put("timestamp", ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
        return KeyValue.pair(key, Jsonizer.toJsonMinified(parsedJson));
    }

    private void onRecord(String key, String value, TopicElasticsearchMapping mapping, Config config) {
        String uri = "http://" + Paths.get(
                config.elasticsearchServer,
                mapping.elasticsearchIndex,
                mapping.elasticsearchType,
                generateKey(mapping, key)).toString();
        System.out.println("curl -XPUT -H 'Content-Type: application/json' " + uri + " -d '" + value + "'");
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(uri))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyProcessor.fromString(value)).build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandler.asString());
            if (response.statusCode() >= 300 || response.statusCode() < 200) {
                System.out.println("error putting in elasticsearch " + response.statusCode() + " " + response.body());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String generateKey(TopicElasticsearchMapping mapping, String key) {
        if (mapping.elasticsearchIdStrategy == null || "random".equalsIgnoreCase(mapping.elasticsearchIdStrategy.trim())) {
            return UUID.randomUUID().toString();
        } else if ("fromfield".equalsIgnoreCase(mapping.elasticsearchIdStrategy.trim())) {
            throw new IllegalStateException("not implemented, but would get key from " + String.valueOf(mapping.idFromField));
        } else if ("fromKey".equalsIgnoreCase(mapping.elasticsearchIdStrategy.trim())) {
            return key;
        } else {
            throw new IllegalStateException("unknown strategy" + String.valueOf(mapping.elasticsearchIdStrategy));
        }
    }
}
