package nl.toefel.kafka.elasticsearch.pump;

import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import nl.toefel.kafka.elasticsearch.pump.kafka.KafkaStringConsumer;
import nl.toefel.kafka.elasticsearch.pump.sink.ElasticsearchBulkHttpSink;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConfigurableStringConsumer implements ConfigurableKafkaSource {

    private Lock lock = new ReentrantLock();
    private Optional<KafkaStringConsumer> oneTryConsumer = Optional.empty();
    private Config cfg;

    /**
     * Reconfigures the streams without waiting if a reconfiguration is already in progress.
     *
     * @param config
     * @return true if successful, false if a reconfiguration was already in progress.
     */
    @Override
    public boolean reconfigureOrCancel(Config config) {
        if (lock.tryLock()) {
            try {
                reconfigure(config);
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
     *
     * @param config
     */
    @Override
    public void reconfigureOrWait(Config config) {
        lock.lock();
        try {
            reconfigure(config);
        } finally {
            lock.unlock();
        }
    }

    private void reconfigure(Config config) {
        System.out.println("Using config: \n" + Jsonizer.toJsonFormatted(config));
        if (config.isEmpty()) {
            System.out.println("Config is empty, not starting any streams");
            return;
        }
        if (!config.isValid()) {
            System.out.println("Not altering config due to invalid parameters");
            return;
        }
        oneTryConsumer.ifPresent(consumer -> consumer.stopSync(5, TimeUnit.SECONDS));
        cfg = config;
        KafkaStringConsumer.newStartedConsumer(cfg, new ElasticsearchBulkHttpSink(cfg));
    }


    @Override
    public Config getConfig() {
        lock.lock();
        try {
            return cfg;
        } finally {
            lock.unlock();
        }
    }
}
