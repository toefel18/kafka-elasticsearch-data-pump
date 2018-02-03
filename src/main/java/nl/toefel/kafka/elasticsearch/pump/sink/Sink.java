package nl.toefel.kafka.elasticsearch.pump.sink;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Sink {

    void process(ConsumerRecords<String, String> batch);
}
