package nl.toefel.kafka.elasticsearch.pump.sink;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.config.TopicElasticsearchMapping;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import nl.toefel.patan.api.Stopwatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static nl.toefel.kafka.elasticsearch.pump.statistics.Statistics.STATS;

public class ElasticsearchBulkHttpSink implements Sink {

    private final HttpClient http = HttpClient.newHttpClient();
    private final Config config;
    private final Map<String, TopicElasticsearchMapping> mappingByTopic;
    private final Map<String, ElasticsearchRecordIdStrategy> idStrategyByTopic;

    public ElasticsearchBulkHttpSink(Config config, ElasticsearchIdFactory idFactory) {
        this.config = config;
        this.mappingByTopic = this.config.topicMappings.stream().collect(toMap(x -> x.topic, Function.identity()));
        this.idStrategyByTopic = this.config.topicMappings.stream().collect(toMap(x -> x.topic, idFactory::getStrategy));
    }

    @Override
    public void process(ConsumerRecords<String, String> batch) {
        STATS.engine.recordElapsedTime("elasticsearch.bulk.http.sink.process.batch", () -> {
            Map<String, List<ConsumerRecord<String, String>>> recordBatchByTopic = groupRecordsByTopic(batch);

            recordBatchByTopic.keySet()
                    .stream()
                    .filter(mappingByTopic::containsKey) // defensively filter any topics for which we do not have a mapping
                    .forEach(topic -> sendToElasticsearch(mappingByTopic.get(topic), recordBatchByTopic.get(topic)));
        });
    }

    private Map<String, List<ConsumerRecord<String, String>>> groupRecordsByTopic(ConsumerRecords<String, String> batch) {
        // add all items to a list so we can use the stream API
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        batch.iterator().forEachRemaining(records::add);

        // groupBy topic
        return records.stream().collect(groupingBy(ConsumerRecord::topic));
    }

    private void sendToElasticsearch(TopicElasticsearchMapping mappingConfig, List<ConsumerRecord<String, String>> consumerRecords) {
        STATS.engine.addOccurrence("elasticsearch.bulk.http.sink.send.to.elasticsearch.topic." + mappingConfig.topic);
        for (ConsumerRecord<String, String> record : consumerRecords) {
            STATS.engine.recordElapsedTime("elasticsearch.bulk.http.sink.process.single", () -> {
                try {
                    Map<String, Object> parsedRecord = Jsonizer.fromJson(record.value());
                    parsedRecord = unifyTimestamps(parsedRecord);
                    if (Boolean.TRUE.equals(mappingConfig.addKafkaMetaData)) {
                        parsedRecord = insertKafkaRecordFieldsAsMeta(parsedRecord, record);
                    }
                    ElasticsearchRecordIdStrategy idStrategy = idStrategyByTopic.get(mappingConfig.topic);
                    String json = Jsonizer.toJsonMinified(parsedRecord);
                    http(mappingConfig, idStrategy.getElkHttpMethod(), idStrategy.getElasticSearchId(record), json);
                } catch (Exception e) {
                    System.out.println("Error transforming to json, skipping record: " + record.value());
                }
            });
        }
    }

    // modifies the input map, and returns the same instance!
    private Map<String, Object> unifyTimestamps(Map<String, Object> parsedRecord) {
        Object timestamp = parsedRecord.get("timestamp");
        if (timestamp != null) {
            parsedRecord.put("originalTimestampInMessage", timestamp);
        }
        if (timestamp instanceof Long) {
            parsedRecord.put("timestamp", ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long) timestamp), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        } else if (timestamp instanceof String) {
            // nothing to do
        } else {
            String generatedTimestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            System.out.println("INCOMPATIBLE TIMESTAMP FORMAT " + timestamp + ", defaulting to now(): " + generatedTimestamp);
            parsedRecord.put("timestamp", generatedTimestamp);
        }
        return parsedRecord;
    }

    private Map<String, Object> insertKafkaRecordFieldsAsMeta(Map<String, Object> parsedRecord, ConsumerRecord<String, String> record) {
        HashMap<String, Object> meta = new HashMap<>();
        meta.put("topic", record.topic());
        meta.put("partition", record.partition());
        meta.put("key", record.key());
        meta.put("offset", record.offset());
        meta.put("serializedValueSize", record.serializedValueSize());
        meta.put("serializedKeySize", record.serializedKeySize());
        meta.put("timestamp", record.timestamp());
        parsedRecord.put("_kafkaMetaData", meta);
        return parsedRecord;
    }

    private void http(TopicElasticsearchMapping mappingConfig, String method, String idInElk, String json) throws IOException, InterruptedException {
        String uri = "http://" + Paths.get(
                config.elasticsearchServer,
                mappingConfig.elasticsearchIndex,
                mappingConfig.elasticsearchType,
                idInElk);

        HttpRequest request = HttpRequest.newBuilder(URI.create(uri))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyProcessor.fromString(json))
                .build();

        if ("always".equalsIgnoreCase(mappingConfig.logCurlCommands)) {
            System.out.println(toCurlCommand(request, json));
        }
        Stopwatch sw = STATS.engine.startStopwatch();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandler.asString());
        STATS.engine.recordElapsedTime("elasticsearch.bulk.http.sink.httpcall." + method, sw);
        logHttpResult(mappingConfig, request, json, response); // TODO built-in retry on failure, throw RetryException and defer to consumer
    }

    private void logHttpResult(TopicElasticsearchMapping mappingConfig, HttpRequest request, String json, HttpResponse<String> response) {
        if (response.statusCode() >= 300 || response.statusCode() < 200) {
            String retryCurl = "";
            if ("onFailure".equalsIgnoreCase(mappingConfig.logCurlCommands)) {
                retryCurl = "\n - manual retry: " + toCurlCommand(request, json);
            }
            System.out.println("http error status=" + response.statusCode() + ", body=" + response.body() + retryCurl);
            STATS.engine.addOccurrence("elasticsearch.bulk.http.sink.httpcall" + request.method() + ".failed");
            STATS.engine.addOccurrence("elasticsearch.bulk.http.sink.httpcall" + request.method() + ".failed." + response.statusCode());
        } else {
            STATS.engine.addOccurrence("elasticsearch.bulk.http.sink.httpcall" + request.method() + ".ok");
        }
    }

    private static String toCurlCommand(HttpRequest request, String body) {
        return String.format("curl -X%s -H 'Content-Type: application/json' %s -d '%s'", request.method(), request.uri().toString(), body);
    }
}
