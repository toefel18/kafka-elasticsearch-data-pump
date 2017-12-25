package nl.toefel.kafka.elasticsearch.pump;

import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.http.RestApiServer;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Christophe Hesters
 */
public class Main {

    public static void main(String[] args) throws IOException {
        Config cfg = Config.newEmpty();
        if (Files.exists(Config.CONFIG_PATH)) {
            cfg = Jsonizer.fromJson(Files.readAllBytes(Config.CONFIG_PATH), Config.class);
        }

        ConfigurableStreams streams = new ConfigurableStreams();
        RestApiServer srv = new RestApiServer(streams);
        srv.start();
        try {
            streams.reconfigureStreamsOrWait(cfg);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
