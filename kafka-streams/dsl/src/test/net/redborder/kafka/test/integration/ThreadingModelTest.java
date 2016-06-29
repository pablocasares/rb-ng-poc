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
import static org.junit.Assert.assertTrue;


public class ThreadingModelTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();


    @BeforeClass
    public static void startKafkaCluster() throws Exception {

        //Start topics with 10 partition and replication equals to 1

        CLUSTER.createTopic(Constants.FLOW_TOPIC, 10, 1);
        CLUSTER.createTopic(Constants.LOCATION_TOPIC, 10, 1);
        CLUSTER.createTopic(Constants.REPUTATION_TOPIC, 10, 1);
        CLUSTER.createTopic(Constants.OUTPUT_TOPIC, 10, 1);
        CLUSTER.createTopic(Constants.REPARTITION_TOPIC, 10, 1);
    }


    @Test
    public void ThreadingModelTest() throws Exception {


        // Input 1: Flow Data
        List<KeyValue<String, String>> flow_data = Arrays.asList(
                new KeyValue<>("client_A", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_A\",\"ip\":\"10.0.0.2\"}"),
                new KeyValue<>("client_B", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_B\",\"ip\":\"10.0.0.3\"}"),
                new KeyValue<>("client_C", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_C\",\"ip\":\"10.0.0.4\"}"),
                new KeyValue<>("client_D", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_D\",\"ip\":\"10.0.0.5\"}"),
                new KeyValue<>("client_E", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_E\",\"ip\":\"10.0.0.6\"}"),
                new KeyValue<>("client_F", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_F\",\"ip\":\"10.0.0.7\"}"),
                new KeyValue<>("client_G", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_G\",\"ip\":\"10.0.0.8\"}"),
                new KeyValue<>("client_H", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_H\",\"ip\":\"10.0.0.9\"}"),
                new KeyValue<>("client_I", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_I\",\"ip\":\"10.0.0.10\"}"),
                new KeyValue<>("client_J", "{\"application\":\"application_A\",\"bytes\":123456789,\"client\":\"client_J\",\"ip\":\"10.0.0.11\"}")
        );

        // Input 2: Location Data
        List<KeyValue<String, String>> location_data = Arrays.asList(
                new KeyValue<>("client_A", "{\"client\":\"client_A\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_B", "{\"client\":\"client_B\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_C", "{\"client\":\"client_C\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_D", "{\"client\":\"client_D\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_E", "{\"client\":\"client_E\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_F", "{\"client\":\"client_F\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_G", "{\"client\":\"client_G\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_H", "{\"client\":\"client_H\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_I", "{\"client\":\"client_I\",\"building\":\"building_A\",\"campus\":\"campus_A\"}"),
                new KeyValue<>("client_J", "{\"client\":\"client_J\",\"building\":\"building_A\",\"campus\":\"campus_A\"}")
        );

        // Input 3: Reputation Data
        List<KeyValue<String, String>> reputation_data = Arrays.asList(
                new KeyValue<>("10.0.0.2", "{\"ip\":\"10.0.0.2\",\"score\":80}"),
                new KeyValue<>("10.0.0.3", "{\"ip\":\"10.0.0.3\",\"score\":80}"),
                new KeyValue<>("10.0.0.4", "{\"ip\":\"10.0.0.4\",\"score\":80}"),
                new KeyValue<>("10.0.0.5", "{\"ip\":\"10.0.0.5\",\"score\":80}"),
                new KeyValue<>("10.0.0.6", "{\"ip\":\"10.0.0.6\",\"score\":80}"),
                new KeyValue<>("10.0.0.7", "{\"ip\":\"10.0.0.7\",\"score\":80}"),
                new KeyValue<>("10.0.0.8", "{\"ip\":\"10.0.0.8\",\"score\":80}"),
                new KeyValue<>("10.0.0.9", "{\"ip\":\"10.0.0.9\",\"score\":80}"),
                new KeyValue<>("10.0.0.10", "{\"ip\":\"10.0.0.10\",\"score\":80}"),
                new KeyValue<>("10.0.0.11", "{\"ip\":\"10.0.0.11\",\"score\":80}")

                );


        ObjectMapper objectMapper = new ObjectMapper();


        // Expected data
        List<KeyValue<String, Map<String, Object>>> expectedEnrichment = Arrays.asList(
                new KeyValue<>("client_A", objectMapper.readValue("{\"client\":\"client_A\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.2\",\"score\":80}", Map.class)),
                new KeyValue<>("client_B", objectMapper.readValue("{\"client\":\"client_B\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.3\",\"score\":80}", Map.class)),
                new KeyValue<>("client_C", objectMapper.readValue("{\"client\":\"client_C\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.4\",\"score\":80}", Map.class)),
                new KeyValue<>("client_D", objectMapper.readValue("{\"client\":\"client_D\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.5\",\"score\":80}", Map.class)),
                new KeyValue<>("client_E", objectMapper.readValue("{\"client\":\"client_E\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.6\",\"score\":80}", Map.class)),
                new KeyValue<>("client_F", objectMapper.readValue("{\"client\":\"client_F\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.7\",\"score\":80}", Map.class)),
                new KeyValue<>("client_G", objectMapper.readValue("{\"client\":\"client_G\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.8\",\"score\":80}", Map.class)),
                new KeyValue<>("client_H", objectMapper.readValue("{\"client\":\"client_H\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.9\",\"score\":80}", Map.class)),
                new KeyValue<>("client_I", objectMapper.readValue("{\"client\":\"client_I\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.10\",\"score\":80}", Map.class)),
                new KeyValue<>("client_J", objectMapper.readValue("{\"client\":\"client_J\",\"application\":\"application_A\",\"bytes\":123456789,\"building\":\"building_A\",\"campus\":\"campus_A\",\"ip\":\"10.0.0.11\",\"score\":80}", Map.class))
                );

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        //Set number of threads to 4
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

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

        Comparator<KeyValue<String, Map<String, Object>>> keyValueComparator = (o1, o2)->o1.key.compareTo(o2.key);

        actualEnrichment.sort(keyValueComparator);
        expectedEnrichment.sort(keyValueComparator);

        assertThat(actualEnrichment, equalTo(expectedEnrichment));
    }


}