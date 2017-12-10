package nl.toefel.kafka.elasticsearch.pump.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * @author Christophe Hesters
 */
public class Jsonizer {

    private static final ObjectMapper minifiedMapper;

    static {
        minifiedMapper = new ObjectMapper();
        minifiedMapper.registerModules(new Jdk8Module(), new JavaTimeModule(), new ParameterNamesModule());
    }

    public static String toJson(Object object, boolean format) {
        ObjectWriter w = format ? minifiedMapper.writerWithDefaultPrettyPrinter() : minifiedMapper.writer();
        try {
            return w.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String toJsonFormatted(Object object) {
        return toJson(object, true);
    }

    public static String toJsonMinified(Object object) {
        return toJson(object, false);
    }
}
