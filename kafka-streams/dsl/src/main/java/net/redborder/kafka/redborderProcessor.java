package net.redborder.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsConfig;
import net.redborder.kafka.utils.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class RedborderProcessor {

    // POJO classes
    static public class Flow {
        public String client;
        public String ip;
        public String app;
        public Long bytes;
    }

    static public class Location {
        public String client;
        public String campus;
        public String building;
    }

    static public class Reputation {
        public String ip;
        public Integer score;
    }

    static public class Enrichment {
        public String client;
        public String ip;
        public String campus;
        public String building;
        public Integer score;
    }



    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-typed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");


        //Define the topology builder

        KStreamBuilder builder = new KStreamBuilder();

        // TODO: the following can be removed with a serialization factory
        Map<String, Object> serdeProps = new HashMap<>();


        //Define the Serdes for all classes (one class for each topic)

        final Serializer<Flow> flowSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Flow.class);
        flowSerializer.configure(serdeProps, false);

        final Deserializer<Flow> flowDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Flow.class);
        flowDeserializer.configure(serdeProps, false);

        final Serde<Flow> flowSerde = Serdes.serdeFrom(flowSerializer, flowDeserializer);

        final Serializer<Location> locationSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Location.class);
        locationSerializer.configure(serdeProps, false);

        final Deserializer<Location> locationDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Location.class);
        locationDeserializer.configure(serdeProps, false);

        final Serde<Location> locationSerde = Serdes.serdeFrom(locationSerializer, locationDeserializer);

        final Serializer<Reputation> reputationSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Reputation.class);
        reputationSerializer.configure(serdeProps, false);

        final Deserializer<Reputation> reputationDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Reputation.class);
        reputationDeserializer.configure(serdeProps, false);

        final Serde<Reputation> reputationSerde = Serdes.serdeFrom(reputationSerializer, reputationDeserializer);

        final Serializer<Enrichment> enrichmentSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Enrichment.class);
        enrichmentSerializer.configure(serdeProps, false);

        final Deserializer<Enrichment> enrichmentDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Enrichment.class);
        enrichmentDeserializer.configure(serdeProps, false);

        final Serde<Enrichment> enrichmentSerde = Serdes.serdeFrom(enrichmentSerializer, enrichmentDeserializer);



        /* Define the sources. In this example flows2 is a KStream. That means that we will use flows2 as an unbounded source of Data.
        * flows2 data will be enrich with locations2 and reputations3 data. These 2 topics are defined as "KTable" meaning that we will
        * store only the last value for a given class (and we will be able to use those values for enrichment)
        *
        * */

        KStream<String, Flow> flows = builder.stream(Serdes.String(), flowSerde, "flows2");

        KTable<String, Location> locations = builder.table(Serdes.String(), locationSerde, "locations2");

        KTable<String, Reputation> reputations = builder.table(Serdes.String(), reputationSerde, "reputations3");


        /*The code above reads from flows topic, do a leftJoin with locations using the messages keys and remap the keys to "ip".
        * Then the repartitioned data is sent to "re-partitioning-topic" topic. We need to ensure that "re-partitioning-topic" have
        * the same partitions than "reputations3" topic in order to be able to do the last leftJoin. Finally the last leftJoin joins the
        * data from re-partitioning-topic with "reputation" data.*/

        System.out.println("Before...");
        KStream<String, Enrichment> enrichmentKStream = flows.leftJoin(locations, (flow, location) -> {
            System.out.println("Doing ENRICHMENT with location...");
            Enrichment enrichment = new Enrichment();
            enrichment.client = flow.client;
            enrichment.ip = flow.ip;
            if(location != null){
                enrichment.building = location.building;
                enrichment.campus = location.campus;
            }
            return enrichment;
        }).map((dummy, record) -> new KeyValue<>(record.ip, record)).through(Serdes.String(), enrichmentSerde, "re-partioning-topic")
        .leftJoin(reputations, (enrichment, reputation) -> {
                System.out.println("Doing ENRICHMENT with reputation...");
                if(reputation != null){
                    enrichment.score = reputation.score;
                }
                return enrichment;
                }).map((dummy, record) -> new KeyValue<>(record.client, record));





        /* Finally we store the enriched data to "enriched-output" topic*/
        enrichmentKStream.to(Serdes.String(), enrichmentSerde, "enriched-output");

        /* This line builds the KafkaStreams process*/
        KafkaStreams streams = new KafkaStreams(builder, props);
        /* This line starts the processing topology*/
        streams.start();

    }
}
