package nl.toefel.kafka.elasticsearch.pump.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class AutoIdStrategy implements ElasticsearchRecordIdStrategy {

    @Override
    public String getElasticSearchId(ConsumerRecord<String, String> record) {
        return "";
    }

    @Override
    public String getElkHttpMethod() {
        return "POST";
    }
}
