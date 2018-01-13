package nl.toefel.kafka.elasticsearch.pump;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.config.TopicElasticsearchMapping;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import nl.toefel.patan.StatisticsFactory;
import nl.toefel.patan.api.Statistics;
import nl.toefel.patan.api.Stopwatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Christophe Hesters
 */
public class ConfigurableStreams {

    private static final String TYPE_CONFIG = "{\"mappings\":{\"${TYPE}\":{\"properties\":{\"timestamp\":{\"type\":\"date\"}}}}}";

    public static final Statistics STATS = StatisticsFactory.createThreadsafeStatistics();
    private final HttpClient http = HttpClient.newHttpClient();
    private KafkaStreams activeStreams;
    private Lock lock = new ReentrantLock();
    private Topology topology = null;
    private Config config = Config.newEmpty();

    /**
     * Reconfigures the streams without waiting if a reconfiguration is already in progress.
     * @param config
     * @return true if successful, false if a reconfiguration was already in progress.
     */
    public boolean reconfigureStreamsOrCancel(Config config) {
        if (lock.tryLock()) {
            try {
                reconfigureStreamsImpl(config);
                return true;
            } finally {
                lock.unlock();
            }
        } else {
            return false;
        }
    }

    /**
     * Reconfigures the streams or waits until streams is ready if a configuration is already in progress.
     * @param config
     */
    public void reconfigureStreamsOrWait(Config config) {
        lock.lock();
        try {
            reconfigureStreamsImpl(config);
        }finally {
            lock.unlock();
        }
    }

    private void reconfigureStreamsImpl(Config config) {
        System.out.println("Using config: \n" + Jsonizer.toJsonFormatted(config));
        if (config.isEmpty()) {
            System.out.println("Config is empty, not starting any streams");
            return;
        }
        if (!config.isValid()) {
            System.out.println("Not altering config due to invalid parameters");
            return;
        }
        if (activeStreams != null) {
            System.out.println("Closing active streams");
            activeStreams.close();
        }
        this.config = config;
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

        topology = builder.build();
        System.out.println(topology.describe());

        activeStreams = new KafkaStreams(topology, props);
        persistConfig(config);
        System.out.println("Starting KafkaStreams");
        activeStreams.start();
        System.out.println("KafkaStreams Started");
    }

    private void persistConfig(Config config) {
        try {
            System.out.println("Persisting configuration");
            Files.deleteIfExists(Config.CONFIG_PATH);
            Files.createFile(Config.CONFIG_PATH);
            Files.write(Config.CONFIG_PATH, Jsonizer.toJsonFormattedBytes(config));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void preprocess(TopicElasticsearchMapping mapping, Config config) {
        Stopwatch sw = STATS.startStopwatch();
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
                STATS.recordElapsedTime("configurable.streams.preprocess.ok", sw);
            } catch (IOException | InterruptedException e) {
                System.out.println("Failed to send type configuration request to " + uri + ", with body: " + body);
                STATS.recordElapsedTime("configurable.streams.preprocess.failed", sw);
            }
        }
    }

    //TODO optimize double json parsing
    private boolean validJsonValues(Object key, Object value) {
        try {
            Jsonizer.fromJson(String.valueOf(value));
            return true;
        } catch (Exception e) {
            System.out.println("Skipping message, not valid JSON. key: " + String.valueOf(key) + ", value: " + String.valueOf(value));
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
            // nothing to do
        } else {
            System.out.println("INCOMPATIBLE TIMESTAMP FORMAT " + timestamp);
            parsedJson.put("timestamp", ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
        return KeyValue.pair(key, Jsonizer.toJsonMinified(parsedJson));
    }

    private void onRecord(String key, String value, TopicElasticsearchMapping mapping, Config config) {
        Stopwatch sw = STATS.startStopwatch();
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
            STATS.recordElapsedTime("on.record." + mapping.topic + ".ok", sw);
        } catch (IOException | InterruptedException e) {
            STATS.recordElapsedTime("on.record." + mapping.topic + ".failed", sw);
            e.printStackTrace();
        }
    }

    private String generateKey(TopicElasticsearchMapping mapping, String key) {
        if (mapping.elasticsearchIdStrategy == null || "auto".equalsIgnoreCase(mapping.elasticsearchIdStrategy)){
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#_automatic_id_generation
            return ""; // this will PUT on /, causing Elasticsearch to automatically determine ID.
        } else if ("random".equalsIgnoreCase(mapping.elasticsearchIdStrategy.trim())) {
            return UUID.randomUUID().toString();
        } else if ("fromField".equalsIgnoreCase(mapping.elasticsearchIdStrategy.trim())) {
            throw new IllegalStateException("not implemented, but would get key from " + String.valueOf(mapping.idFromField));
        } else if ("fromKey".equalsIgnoreCase(mapping.elasticsearchIdStrategy.trim())) {
            return key;
        } else {
            System.out.println("unknown strategy " + String.valueOf(mapping.elasticsearchIdStrategy) + ", defaulting to auto");
            return "";
        }
    }

    public Optional<TopologyDescription> getTopologyDescription() {
        if (topology != null) {
            return Optional.of(topology.describe());
        } else {
            return Optional.empty();
        }
    }

    public Config getConfig() {
        return config;
    }
}
