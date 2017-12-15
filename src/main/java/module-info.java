module nl.toefel.kafka.elasticsearch.pump {
    requires kafka.streams;
    requires kafka.clients;
    requires com.fasterxml.jackson.databind;
    requires org.slf4j;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.fasterxml.jackson.module.paramnames;
    requires com.fasterxml.jackson.core;
    requires jdk.incubator.httpclient;
    requires patan;
    requires jackson.annotations;

    exports nl.toefel.kafka.elasticsearch.pump;
    exports nl.toefel.kafka.elasticsearch.pump.config;
}