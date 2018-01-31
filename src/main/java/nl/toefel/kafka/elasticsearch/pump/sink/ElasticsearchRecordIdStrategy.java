package nl.toefel.kafka.elasticsearch.pump.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ElasticsearchRecordIdStrategy {
    String getElasticSearchId(ConsumerRecord<String, String> record);
    String getElkHttpMethod();
}
