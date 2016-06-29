package net.redborder.kafka.test.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
import net.redborder.kafka.FlowLocationJoiner;
import net.redborder.kafka.FlowReputationJoiner;
import net.redborder.kafka.RedborderProcessor;
import net.redborder.kafka.utils.Constants;
import net.redborder.kafka.utils.JsonDeserializer;
import net.redborder.kafka.utils.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;


import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;


public class EnrichmentTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Constants.FLOW_TOPIC);
        CLUSTER.createTopic(Constants.LOCATION_TOPIC);
        CLUSTER.createTopic(Constants.REPUTATION_TOPIC);
        CLUSTER.createTopic(Constants.OUTPUT_TOPIC);
    }


    @Test
    public void enrichmentTest() throws Exception {


        // Input 1: Flow Data
        List<KeyValue<String, String>> flow_data = Arrays.asList(
                new KeyValue<>("client_A", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_A\",\"ip\":\"10.0.0.2\"}")
        );

        // Input 2: Location Data
        List<KeyValue<String, String>> location_data = Arrays.asList(
                new KeyValue<>("client_A", "{\"client\":\"client_A\",\"building\":\"building_A\",\"campus\":\"campus_A\"}")

        );

        // Input 3: Reputation Data
        List<KeyValue<String, String>> reputation_data = Arrays.asList(
                new KeyValue<>("10.0.0.2", "{\"ip\":\"10.0.0.2\",\"score\":80}")
        );


        ObjectMapper objectMapper = new ObjectMapper();


        // Expected data
        List<KeyValue<String, Map<String, Object>>> expectedEnrichment = Arrays.asList(
                new KeyValue<>("client_A", objectMapper.readValue("{\"client\":\"client_A\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.2\",\"score\":80}", Map.class))

        );

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());

        RedborderProcessor.process(streamsConfiguration);

        //
        // Step 2: Publish flows information.
        //
        // To keep this code example simple and easier to understand/reason about, we publish all
        // flows records before any locations records (cf. step 3).  In practice though,
        // data records would typically be arriving concurrently in both input streams/topics.
        Properties flowsProducerConfig = new Properties();
        flowsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        flowsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        flowsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        flowsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        flowsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(Constants.FLOW_TOPIC, flow_data, flowsProducerConfig);
        //
        // Step 3: Publish some location events.
        //
        Properties locationsProducerConfig = new Properties();
        locationsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        locationsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        locationsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        locationsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        locationsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(Constants.LOCATION_TOPIC, location_data, locationsProducerConfig);


        //
        // Step 4: Publish some reputation events.
        //
        Properties reputationsProducerConfig = new Properties();
        reputationsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        reputationsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        reputationsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        reputationsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        reputationsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(Constants.REPUTATION_TOPIC, reputation_data, reputationsProducerConfig);

        // Give the stream processing application some time to do its work.


        //
        // Step 4: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "join-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        List<KeyValue<String, Map<String, Object>>> actualEnrichment = IntegrationTestUtils.readKeyValues(Constants.OUTPUT_TOPIC, consumerConfig);


        Thread.sleep(10000);

        RedborderProcessor.closeStreams();

        assertThat(actualEnrichment, equalTo(expectedEnrichment));
    }

}