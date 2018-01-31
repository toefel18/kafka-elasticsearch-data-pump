package nl.toefel.kafka.elasticsearch.pump.kafka;

import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.sink.Sink;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static nl.toefel.kafka.elasticsearch.pump.statistics.Statistics.STATS;

/**
 * Implementation based on article:
 * https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/
 */
public class KafkaStringConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final Sink sink;
    private final CountDownLatch latch = new CountDownLatch(1);

    public static KafkaStringConsumer newStartedConsumer(Config cfg, Sink sink) {
        KafkaStringConsumer consumer = new KafkaStringConsumer(cfg, sink);
        List<String> topics = cfg.topicMappings.stream().map(mapping -> mapping.topic).collect(Collectors.toList());
        System.out.println("Starting a new kafka consumer with a subscription to topics " + topics);
        consumer.consumer.subscribe(topics);
        new Thread(consumer::pollForMessages).start();
        STATS.engine.addOccurrence("kafka.string.consumer.created");
        return consumer;
    }

    private KafkaStringConsumer(Config cfg, Sink sink) {
        this.consumer = createConsumer(cfg);
        this.sink = sink;
    }

    private KafkaConsumer<String, String> createConsumer(Config cfg) {
        Properties props = new Properties();
        props.put("bootstrap.servers", cfg.kafkaBootstrapServers);
        props.put("group.id", cfg.kafkaConsumerGroupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public void stopSync(long timeout, TimeUnit timeUnit) {
        STATS.engine.recordElapsedTime("kafka.string.consumer.stop.sync", () -> {
            System.out.println("Closing kafka consumer with a maximum wait of " + timeout + " " + timeUnit.name());
            consumer.wakeup();
            try {
                boolean consumerClosedProperly = latch.await(timeout, timeUnit);
                System.out.println(consumerClosedProperly? "Kafka consumer stopped correctly" : "Kafka consumer did not respond to stop, something appears to be wrong");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    private void pollForMessages() {
        System.out.println("Entering poll loop until consumer explicitly stopped");
        try {
            // polls forever until a message is available, or throws WakeupException when .wakeup() is called via stopSync
            while (true) {
                STATS.engine.recordElapsedTime("kafka.string.consumer.poll.for.messages", () -> {
                    ConsumerRecords<String, String> records =
                            STATS.engine.recordElapsedTime("kafka.string.consumer.poll.for.messages.poll", () -> consumer.poll(Long.MAX_VALUE));
                    STATS.engine.recordElapsedTime("kafka.string.consumer.poll.for.messages.processExceptionSafe", () -> processExceptionSafe(records));
                    STATS.engine.recordElapsedTime("kafka.string.consumer.poll.for.messages.commit.sync", () -> consumer.commitSync()); // TODO if this throws, we should not exit poll loop but wait for reconnect
                });
            }
        } catch (WakeupException e) {
            // only happends when somebody calls .wakeup() on the consumer, indicating a shutdown
        } finally {
            close();
        }
    }

    private void processExceptionSafe(ConsumerRecords<String, String> records) {
        try {
            sink.process(records);
        } catch (Exception e) {
            // TODO lacking a proper retry mechanism on valid backend errors, consumer should be paused and resumed
            e.printStackTrace();
        }
    }

    private void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            e.printStackTrace(); // should not happen, but if it does i want to know why
        } finally {
            latch.countDown();
        }
    }
}
