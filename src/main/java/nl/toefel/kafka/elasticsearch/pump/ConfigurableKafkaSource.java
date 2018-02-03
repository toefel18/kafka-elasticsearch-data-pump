package nl.toefel.kafka.elasticsearch.pump;

import nl.toefel.kafka.elasticsearch.pump.config.Config;

public interface ConfigurableKafkaSource {
    boolean reconfigureOrCancel(Config config);

    void reconfigureOrWait(Config config);

    Config getConfig();
}
