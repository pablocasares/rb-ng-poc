package net.redborder.kafka;


import net.redborder.kafka.utils.Constants;
import net.redborder.kafka.utils.JsonDeserializer;
import net.redborder.kafka.utils.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;


public class RedborderProcessor {


    private static KafkaStreams streams;

    public static void process(Properties streamsConfiguration) throws Exception {



        //Define the topology builder

        KStreamBuilder builder = new KStreamBuilder();


        JsonSerializer jsonSerializer = new JsonSerializer();
        JsonDeserializer jsonDeserializer = new JsonDeserializer();


        //Define the Serdes

        final Serde<Map<String, Object>> inputSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final Serde<Map<String, Object>> enrichmentSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);



        /* Define the sources. In this example flows is a KStream. That means that we will use flows as an unbounded source of Data.
        * flows data will be enrich with locations2 and reputations3 data. These 2 topics are defined as "KTable" meaning that we will
        * store only the last value for a given key (and we will be able to use those values for enrichment)
        *
        * */

        KStream<String, Map<String, Object>> flows = builder.stream(Serdes.String(), inputSerde, Constants.FLOW_TOPIC);

        KTable<String, Map<String, Object>> locations = builder.table(Serdes.String(), inputSerde, Constants.LOCATION_TOPIC);

        KTable<String, Map<String, Object>> reputations = builder.table(Serdes.String(), enrichmentSerde, Constants.REPUTATION_TOPIC);


        /*The code above reads from flows topic, do a leftJoin with locations using the messages keys and remap the keys to "ip".
        * Then the repartitioned data is sent to "re-partitioning-topic" topic. We need to ensure that "re-partitioning-topic" have
        * the same partitions than "reputations3" topic in order to be able to do the last leftJoin. Finally the last leftJoin joins the
        * data from re-partitioning-topic with "reputation" data.*/

        KStream<String, Map<String, Object>> enrichmentKStream = flows.leftJoin(locations, (flow, location) -> new FlowLocationJoiner().apply(flow, location))
                .map((dummy, record) -> new KeyValue<>((String) record.get("ip"), record))
                .through(Serdes.String(), enrichmentSerde, Constants.REPARTITION_TOPIC)
                .leftJoin(reputations, (enrichment, reputation) -> new FlowReputationJoiner().apply(enrichment, reputation))
                .map((dummy, record) -> new KeyValue<>((String) record.get("client"), record));


        /* Finally we store the enriched data to output topic*/
        enrichmentKStream.to(Serdes.String(), enrichmentSerde, Constants.OUTPUT_TOPIC);

        /* This line builds the KafkaStreams process*/
        streams = new KafkaStreams(builder, streamsConfiguration);
        /* This line starts the processing topology*/
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000L);
    }

    public static void closeStreams(){
        streams.close();
    }
}
