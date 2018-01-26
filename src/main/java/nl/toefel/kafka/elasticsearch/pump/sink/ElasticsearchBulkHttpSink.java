package nl.toefel.kafka.elasticsearch.pump.sink;

import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.config.TopicElasticsearchMapping;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static nl.toefel.kafka.elasticsearch.pump.json.Jsonizer.fromJson;

/**
 *
 */
public class ElasticsearchBulkHttpSink implements Sink {

    private final Config config;
    private final Map<String, TopicElasticsearchMapping> mappingByTopic;

    public ElasticsearchBulkHttpSink(Config config) {
        this.config = config;
        this.mappingByTopic = this.config.topicMappings.stream().collect(toMap(x -> x.topic, Function.identity()));
    }

    @Override
    public void process(ConsumerRecords<String, String> batch) {
        Map<String, List<ConsumerRecord<String, String>>> recordBatchByTopic = groupRecordsByTopic(batch);

        recordBatchByTopic.keySet()
                .stream()
                .filter(mappingByTopic::containsKey) // defensively filter any topics for which we do not have a mapping
                .forEach(topic -> sendToElk(mappingByTopic.get(topic), recordBatchByTopic.get(topic)));
    }

    private Map<String, List<ConsumerRecord<String, String>>> groupRecordsByTopic(ConsumerRecords<String, String> batch) {
        // add all items to a list so we can use the stream API
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        batch.iterator().forEachRemaining(records::add);

        // groupBy topic
        return records.stream().collect(groupingBy(ConsumerRecord::topic));
    }

    private void sendToElk(TopicElasticsearchMapping topicElasticsearchMapping, List<ConsumerRecord<String, String>> consumerRecords) {
        for (ConsumerRecord<String, String> record : consumerRecords) {
            try {
                Map<String, Object> json = Jsonizer.fromJson(record.value());
                unifyTimestamps(json);
                //SEND
            } catch (Exception e) {
                System.out.println("Error transforming to json, skipping record: " + record.value());
            }
        }
    }

    private void unifyTimestamps(Map<String, Object> parsedJson) {
        Object timestamp = parsedJson.get("timestamp");
        if (timestamp != null) {
            parsedJson.put("originalTimestampInMessage", timestamp);
        }
        if (timestamp instanceof Long) {
            parsedJson.put("timestamp", ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long)timestamp), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        } else if (timestamp instanceof String) {
            // nothing to do
        } else {
            String generatedTimestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            System.out.println("INCOMPATIBLE TIMESTAMP FORMAT " + timestamp + ", defaulting to now(): " + generatedTimestamp);
            parsedJson.put("timestamp", generatedTimestamp);
        }
    }
}
