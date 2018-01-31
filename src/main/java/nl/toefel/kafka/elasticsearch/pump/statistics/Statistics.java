package nl.toefel.kafka.elasticsearch.pump.statistics;

import nl.toefel.patan.StatisticsFactory;

public enum Statistics {
    STATS;

    public final nl.toefel.patan.api.Statistics engine = StatisticsFactory.createThreadsafeStatistics();

}
