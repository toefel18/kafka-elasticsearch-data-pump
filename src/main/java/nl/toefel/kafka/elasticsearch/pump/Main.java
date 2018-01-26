package nl.toefel.kafka.elasticsearch.pump;

import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.http.RestApiServer;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import nl.toefel.kafka.elasticsearch.pump.kafka.KafkaStringConsumer;

import java.io.IOException;
import java.nio.file.Files;

/**
 * @author Christophe Hesters
 */
public class Main {

    public static void main(String[] args) throws IOException {
        Config cfg = Config.newEmpty();
        if (Files.exists(Config.CONFIG_PATH)) {
            cfg = Jsonizer.fromJson(Files.readAllBytes(Config.CONFIG_PATH), Config.class);
        }

        ConfigurableKafkaSource kafkaSource = new ConfigurableStringConsumer();

        // port cannot be part of Config, because the REST server receives the config.
        // the dockerfile also maps this port, changing it would render the service unreachable outside the docker network.
        int port = getIntFromEnv("PORT", 8080);
        RestApiServer srv = new RestApiServer(kafkaSource, port);
        srv.start();
        try {
            kafkaSource.reconfigureOrWait(cfg);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Dead");
        }
    }

    private static int getIntFromEnv(String key, int defaultValue) {
        String val = System.getenv(key);
        if (val == null) {
            return defaultValue;
        } else if (!val.matches("^[0-9]+$")) {
            throw new IllegalArgumentException("Environment var " + key + " must be integer, value: " + val);
        } else {
            return Integer.parseInt(val);
        }
    }

}
