package net.redborder.kafka;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Main {

    final static Logger logger = Logger.getLogger(Main.class);

    public static void main(String args[]) {

        try {
            logger.info("Starting location enrichment");
            logger.info("Generating configuration");

            Properties streamsConfiguration = new Properties();
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-flow-location-reputation");
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");

            RedborderProcessor.process(streamsConfiguration);
            logger.info("Location enrichment started");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
