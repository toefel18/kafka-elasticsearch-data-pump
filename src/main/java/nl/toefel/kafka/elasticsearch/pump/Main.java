package nl.toefel.kafka.elasticsearch.pump;

import nl.toefel.kafka.elasticsearch.pump.config.Config;

/**
 * @author Christophe Hesters
 */
public class Main {
    public static void main(String[] args) {
        ConfigurableStreams streams = new ConfigurableStreams();
        streams.reconfigureStreams(new Config());

    }
}
