package net.redborder.kafka;

import org.apache.log4j.Logger;

public class Main {

    final static Logger logger = Logger.getLogger(Main.class);

    public static void main(String args[]) {

        try {
            logger.info("Starting location enrichment");
            RedborderProcessor.process();
            logger.info("Location enrichment started");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
