package net.redborder.kafka;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Map;

public class FlowReputationJoiner implements ValueJoiner<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

    @Override
    public Map<String, Object> apply(Map<String, Object> enrichment, Map<String, Object> reputation) {
        System.out.println("Doing ENRICHMENT with reputation...");
        if (reputation != null) {
            enrichment.put("score", reputation.get("score"));
        }
        return enrichment;
    }
}
