package nl.toefel.kafka.elasticsearch.pump.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.UUID;

public class FromFieldIdStrategy implements ElasticsearchRecordIdStrategy {

    private String jsonPathToField;

    public FromFieldIdStrategy(String jsonPathToField) {
        this.jsonPathToField = jsonPathToField;
        throw new IllegalStateException("FromField not yet implemented");
    }

    @Override
    public String getElasticSearchId(ConsumerRecord<String, String> record) {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getElkHttpMethod() {
        return "PUT";
    }
}
