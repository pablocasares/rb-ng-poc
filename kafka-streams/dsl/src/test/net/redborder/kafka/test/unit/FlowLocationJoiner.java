package net.redborder.kafka.test.unit;

import com.fasterxml.jackson.databind.ObjectMapper;


import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class FlowLocationJoiner {

    private static final ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void FlowLocationJoiner() throws Exception {

        // Input 1: Flow Data
        Map<String, Object> flow_data = objectMapper.readValue("{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_A\",\"ip\":\"10.0.0.2\"}", Map.class);
        Map<String, Object> location_data = objectMapper.readValue("{\"client\":\"client_A\",\"building\":\"building_A\",\"campus\":\"campus_A\"}", Map.class);

        Map<String, Object> actualEnrichment = new net.redborder.kafka.FlowLocationJoiner().apply(flow_data, location_data);

        // Expected data
        Map<String, Object> expectedEnrichment = objectMapper.readValue("{\"client\":\"client_A\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.2\"}", Map.class);

        assertThat(actualEnrichment, equalTo(expectedEnrichment));
    }


}
