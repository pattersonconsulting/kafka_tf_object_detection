package com.pattersonconsultingtn.kafka.examples.tf_object_detection;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.concurrent.TimeUnit; 
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
 
 
 /**

    #### Examples and References

        https://github.com/confluentinc/kafka-streams-examples/blob/4.1.1-post/src/main/java/io/confluent/examples/streams/PageViewRegionExample.java


    ###### To Run this Demo ######

	

        Quick Start

        # (1) Start Zookeeper. Since this is a long-running service, you should run it in its own terminal.
        $ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

        # (2) Start Kafka, also in its own terminal.
        $ ./bin/kafka-server-start ./etc/kafka/server.properties

        # (3) Start the Schema Registry, also in its own terminal.
        ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties



        // (4) create topic for incoming objects

        ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic shopping_cart_objects

        # create topic for aggregate counts

        ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic aggregate_cart_objects


        // (5) Start the Streaming App

            mvn exec:java -Dexec.mainClass=com.pattersonconsultingtn.kafka.examples.tf_object_detection.StreamingDetectedObjectCounts


        // (6) Run the producer from maven

        mvn exec:java -Dexec.mainClass="com.pattersonconsultingtn.kafka.examples.tf_object_detection.ObjectDetectionProducer" \
          -Dexec.args="10 http://localhost:8081 ./src/main/resources/cart_images/"




    	// (7) kafka consumer setup from console

./bin/kafka-console-consumer --bootstrap-server localhost:9092 \
--topic aggregate_cart_objects \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


bin/kafka-console-consumer --topic aggregate_cart_objects --from-beginning \
--new-consumer --bootstrap-server localhost:9092 \
--property print.key=true \
--property print.value=true \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

bin/kafka-console-consumer --topic aggregate_cart_objects --from-beginning \
--new-consumer --bootstrap-server localhost:9092 \
--property print.key=true \
--property print.value=true \
--formatter kafka.tools.DefaultMessageFormatter \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


 */
public class StreamingDetectedObjectCounts {

    final static public String sourceTopicName = "shopping_cart_objects";
    final static public String aggregateDestTopicName = "aggregate_cart_objects";
 
    // based on the average shopping session taking around 30 minutes ...
    // A hopping time window with a size of 15 minutes and an advance interval of 5 minute.
    // The window's name -- the string parameter -- is used to e.g. name the backing state store.
    static final Long shoppingSaleMinutesWindowSize = TimeUnit.MINUTES.toMillis(15); // min
    static final Long shoppingSaleMinutesAdvanceSize = TimeUnit.MINUTES.toMillis(5);


    public static void main(String[] args) throws Exception {
        
        // Streams properties ----- 
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamingCartObjectAggregatorApp");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "KafkaStreamingCartObjectAggregatorApp_Client");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Where to find the Confluent schema registry instance(s)
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //final Serde<String> stringSerde = Serdes.String();
        //final Serde<Long> longSerde = Serdes.Long();

        //long windowSizeMs = TimeUnit.MINUTES.toMillis(15); // 15 * 60 * 1000L
        //long advanceMs =    TimeUnit.MINUTES.toMillis(5); // 5 * 60 * 1000L
        //TimeWindows.of(windowSizeMs).advanceBy(advanceMs);

 
        final StreamsBuilder builder = new StreamsBuilder();
 
        /*
            General stream processing topology for getting summation of objects coming:

                (1) map the generic records by the "class_name" field
                (2) groupByKey() // groups records by class_name
                (3) count( "key" ) // get counts per key
                (4) mapValues() // ???



        */

        // Create a stream of object detection events from the detected_cv_objects_avro topic, where the key of
        // a record is assumed to be the camera-id and the value an Avro GenericRecord
        // that represents the full details of the object detected in an image. 

        final KStream<String, GenericRecord> detectedObjectsKStream = builder.stream( sourceTopicName );

        // Create a keyed stream of object-detect events from the detectedObjectsKStream stream,
        // by extracting the class_name (String) from the Avro value
        final KStream<String, GenericRecord> detectedObjectsKeyedByClassname = detectedObjectsKStream.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
          @Override
          public KeyValue<String, GenericRecord> apply(final String cameraID, final GenericRecord record) {

            System.out.println( "debug: '" + record.get("class_name") + "' " );
            
            return new KeyValue<>(record.get("class_name").toString(), record);
          }
        });

        KGroupedStream<String, GenericRecord> groupedDetectedObjectStream = detectedObjectsKeyedByClassname.groupByKey();

        /**
            here we need to bleed out any old informations so we dont have old shopping data messing up our 
            real-time info


        */
/*
        TimeWindowedKStream<String, GenericRecord> windowedGroupedObjectKStream = groupedDetectedObjectStream.windowedBy( 
                TimeWindows.of( 
                    TimeUnit.MINUTES.toMillis( shoppingSaleMinutesWindowSize )
                    .advanceBy(TimeUnit.MINUTES.toMillis( shoppingSaleMinutesAdvanceSize ) )
                ) 
            );            
*/
        KTable<String, Long> detectedObjectCounts = groupedDetectedObjectStream.count(); 
//        KTable<String, Long> detectedObjectCounts = windowedGroupedObjectKStream.count(); 

        KStream<String, Long> detectedObjectCountsStream = detectedObjectCounts.toStream();

        KStream<String, Long> unmodifiedStream = detectedObjectCountsStream.peek(
            new ForeachAction<String, Long>() {
              @Override
              public void apply(String key, Long value) {
                System.out.println("Post Grouping >> key='" + key + "', value=" + value);
              }
            });        

              
        detectedObjectCountsStream.to( aggregateDestTopicName, Produced.with(Serdes.String(), Serdes.Long()));
 
        //final Topology topology = ;
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
 
        // ... same as Pipe.java above
        Runtime.getRuntime().addShutdownHook(new Thread("pct-object-count-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });   


        try {
            System.out.println( "Starting TF Object Detection Count Streaming App..." );
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}