package nl.toefel.kafka.elasticsearch.pump.http;

import nl.toefel.kafka.elasticsearch.pump.config.Config;

/**
 * @author Christophe Hesters
 */
public class Response {
    public boolean hasErrors;
    public String error;
    public String message;
    public Config config;

    public static Response newError(String message, Config cfg) {
        Response r = new Response();
        r.hasErrors = true;
        r.error = message;
        r.config = cfg;
        return r;
    }

    public static Response newOk(String message, Config cfg) {
        Response r = new Response();
        r.hasErrors = false;
        r.message = message;
        r.config = cfg;
        return r;
    }
}
