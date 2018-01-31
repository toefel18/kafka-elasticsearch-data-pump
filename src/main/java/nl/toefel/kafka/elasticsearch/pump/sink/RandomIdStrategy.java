package nl.toefel.kafka.elasticsearch.pump.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.UUID;

public class RandomIdStrategy implements ElasticsearchRecordIdStrategy {

    @Override
    public String getElasticSearchId(ConsumerRecord<String, String> record) {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getElkHttpMethod() {
        return "PUT";
    }
}
