package nl.toefel.kafka.elasticsearch.pump.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import nl.toefel.kafka.elasticsearch.pump.ConfigurableStreams;
import nl.toefel.kafka.elasticsearch.pump.config.Config;
import nl.toefel.kafka.elasticsearch.pump.json.Jsonizer;
import org.apache.kafka.streams.TopologyDescription;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * @author Christophe Hesters
 */
public class RestApiServer {

    private final ConfigurableStreams streams;
    private HttpServer srv;

    public RestApiServer(ConfigurableStreams streams) {
        this.streams = streams;
        try {
            this.srv = HttpServer.create();
        } catch (IOException e) {
            throw new IllegalStateException("Error creating HTTP server", e);
        }
    }

    public void start() {
        try {
            System.out.println("Starting HTTP server");
            srv.bind(new InetSocketAddress(8080), 0);
            srv.createContext("/configuration", this::handleConfig);
            srv.createContext("/topology", this::handleTopology);
            srv.setExecutor(null);
            srv.start();
            System.out.println("Started HTTP server");
        } catch (IOException e) {
            throw new IllegalStateException("Error starting HTTP server", e);
        }
    }

    private void handleTopology(HttpExchange httpExchange) {
        Optional<TopologyDescription> topo = streams.getTopologyDescription();
        byte[] description = String.valueOf(topo.isPresent() ? topo.get() : "Not found").getBytes();
        try {
            httpExchange.getResponseHeaders().add("Content-Type", "application/json");
            httpExchange.sendResponseHeaders(topo.isPresent() ? 200 : 404, description.length);
            httpExchange.getResponseBody().write(description);
            httpExchange.getResponseBody().flush();
            httpExchange.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleConfig(HttpExchange httpExchange) {
        if (!httpExchange.getRequestURI().getPath().equalsIgnoreCase("/configuration")){
            respond(httpExchange, 404, Response.newError("Invalid URI, use /configuration", null));
        } else if ("GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
            respond(httpExchange, 200, Response.newOk("Listing current config", streams.getConfig()));
        } else if ("PUT".equalsIgnoreCase(httpExchange.getRequestMethod())) {
            try {
                Config newConfig = Jsonizer.fromJson(httpExchange.getRequestBody(), Config.class);
                if (!newConfig.isValid()) {
                    respond(httpExchange, 400, Response.newError("Invalid configuration, required fields: " + Config.requiredFieldsToBeValid(), newConfig));
                } else if (streams.reconfigureStreamsOrCancel(newConfig)) {
                    respond(httpExchange, 202, Response.newOk("Streams are reconfiguring, this can take a while", streams.getConfig()));
                } else {
                    respond(httpExchange, 503, Response.newOk("Reconfiguration already in progress, this API is unavailable until finished", streams.getConfig()));
                }
            } catch (IllegalArgumentException e) {
                respond(httpExchange, 400, Response.newError("Error parsing JSON body as a valid configuration: " + e.getMessage(), null));
            }
        } else {
            respond(httpExchange, 405, Response.newError("Invalid method, only GET or PUT allowed", null));
        }
    }

    private void respond(HttpExchange httpExchange, int statusCode, Response response) {
        System.out.println("responding with " + statusCode + " to " + httpExchange.getRequestMethod() + " " + httpExchange.getRequestURI().getPath());
        try {
            System.out.println(Jsonizer.toJsonFormatted(response));
            byte[] bytesMessage = Jsonizer.toJsonFormattedBytes(response);
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
