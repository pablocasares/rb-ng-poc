package net.redborder.kafka;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.HashMap;
import java.util.Map;

public class FlowLocationJoiner implements ValueJoiner<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

    @Override
    public Map<String, Object> apply(Map<String, Object> flow, Map<String, Object> location) {
        System.out.println("Doing ENRICHMENT with location...");
        Map<String, Object> enrichment = new HashMap<>();
        enrichment.putAll(flow);
        if (location != null) {
            enrichment.put("building", location.get("building"));
            enrichment.put("campus", location.get("campus"));
        }
        return enrichment;
    }
}
