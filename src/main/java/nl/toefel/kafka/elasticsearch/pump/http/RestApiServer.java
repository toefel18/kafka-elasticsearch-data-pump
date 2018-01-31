package nl.toefel.kafka.elasticsearch.pump.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import nl.toefel.kafka.elasticsearch.pump.ConfigurableKafkaSource;
import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;

import java.io.IOException;
import java.net.InetSocketAddress;

import static nl.toefel.kafka.elasticsearch.pump.statistics.Statistics.STATS;

/**
 * @author Christophe Hesters
 */
public class RestApiServer {

    private final ConfigurableKafkaSource kafkaSource;
    private HttpServer srv;
    private int port;

    public RestApiServer(ConfigurableKafkaSource kafkaSource, int port) {
        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("Port number must be in range 1 - 65535, but was " + port);
        if (kafkaSource == null)
            throw new IllegalArgumentException("kafkaSource is null, REST api cannot configure anything");
        this.kafkaSource = kafkaSource;
        this.port = port;
        try {
            this.srv = HttpServer.create();
        } catch (IOException e) {
            throw new IllegalStateException("Error creating HTTP server", e);
        }
    }

    public void start() {
        try {
            System.out.println("Starting HTTP server at port " + port);

            srv.bind(new InetSocketAddress(port), 0);
            srv.createContext("/configuration", this::handleConfig);
            srv.createContext("/statistics", this::handleStats);
            srv.setExecutor(null);
            srv.start();
            System.out.println("Started HTTP server");
        } catch (IOException e) {
            throw new IllegalStateException("Error starting HTTP server", e);
        }
    }

    private void handleConfig(HttpExchange httpExchange) {
        if (!httpExchange.getRequestURI().getPath().equalsIgnoreCase("/configuration")) {
            respond(httpExchange, 404, Response.newError("Invalid URI, use /configuration", null));
        } else if ("GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
            respond(httpExchange, 200, Response.newOk("Listing current config", kafkaSource.getConfig()));
        } else if ("PUT".equalsIgnoreCase(httpExchange.getRequestMethod())) {
            try {
                Config newConfig = Jsonizer.fromJson(httpExchange.getRequestBody(), Config.class);
                if (!newConfig.isValid()) {
                    respond(httpExchange, 400, Response.newError("Invalid configuration, required fields: " + Config.requiredFieldsToBeValid(), newConfig));
                } else if (kafkaSource.reconfigureOrCancel(newConfig)) {
                    respond(httpExchange, 202, Response.newOk("Streams are reconfiguring, this can take a while", kafkaSource.getConfig()));
                } else {
                    respond(httpExchange, 503, Response.newOk("Reconfiguration already in progress, this API is unavailable until finished", kafkaSource.getConfig()));
                }
            } catch (IllegalArgumentException e) {
                respond(httpExchange, 400, Response.newError("Error parsing JSON body as a valid configuration: " + e.getMessage(), null));
            }
        } else {
            respond(httpExchange, 405, Response.newError("Invalid method, only GET or PUT allowed", null));
        }
    }

    private void handleStats(HttpExchange httpExchange) {
        if (!httpExchange.getRequestURI().getPath().equalsIgnoreCase("/statistics")) {
            respond(httpExchange, 404, Response.newError("Invalid URI, use /statistics", null));
        } else if ("GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
            respond(httpExchange, 200, Jsonizer.toJsonFormattedBytes(STATS.engine.getSnapshot()));
        } else if ("DELETE".equalsIgnoreCase(httpExchange.getRequestMethod())) {
            respond(httpExchange, 200, Jsonizer.toJsonFormattedBytes(STATS.engine.getSnapshotAndReset()));
        }
    }

    private void respond(HttpExchange httpExchange, int statusCode, Response response) {
        respond(httpExchange, statusCode, Jsonizer.toJsonFormattedBytes(response));
    }

    private void respond(HttpExchange httpExchange, int statusCode, byte[] bytesMessage) {
        System.out.println("responding with " + statusCode + " to " + httpExchange.getRequestMethod() + " " + httpExchange.getRequestURI().getPath());
        try {
            httpExchange.getResponseHeaders().add("Content-Type", "application/json");
            httpExchange.sendResponseHeaders(statusCode, bytesMessage.length);
            httpExchange.getResponseBody().write(bytesMessage);
            httpExchange.getResponseBody().flush();
            httpExchange.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }
}
