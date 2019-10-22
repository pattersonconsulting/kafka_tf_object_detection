package com.pattersonconsultingtn.kafka.examples.tf_object_detection;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
 
 
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



        // (4) Create topic in Kafka

        ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic shopping_cart_objects

        # detected_cv_objects_counts

        ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic aggregate_cart_objects


        // (5) Start the Streaming App

            mvn exec:java -Dexec.mainClass=com.pattersonconsultingtn.kafka.examples.tf_object_detection.StreamingDetectedObjectCounts


        // (6) Run the producer from maven

        mvn exec:java -Dexec.mainClass="com.pattersonconsultingtn.kafka.examples.tf_object_detection.ObjectDetectionProducer" \
          -Dexec.args="./src/main/resources/cart_images/"




    	// (7) kafka consumer setup from console

./bin/kafka-console-consumer --bootstrap-server localhost:9092 \
--topic detected_cv_objects_counts_2 \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


bin/kafka-console-consumer --topic detected_cv_objects_counts_2 --from-beginning \
--new-consumer --bootstrap-server localhost:9092 \
--property print.key=true \
--property print.value=true \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

bin/kafka-console-consumer --topic detected_cv_objects_counts_2 --from-beginning \
--new-consumer --bootstrap-server localhost:9092 \
--property print.key=true \
--property print.value=true \
--formatter kafka.tools.DefaultMessageFormatter \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


 */
public class StreamingJoin_CartCountsAndInventoryTopics {
	
	final static public String sourceTopicName = "shopping_cart_objects";
	private static Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
 
    public static void main(String[] args) throws Exception {
    	
    	root.setLevel(Level.ERROR);
    	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
        
        // Streams properties ----- 
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pct-cv-streaming-join-counts-inventory-app-3");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "pct-cv-streaming-join-counts-inventory-client");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Where to find the Confluent schema registry instance(s)
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //final Serde<String> stringSerde = Serdes.String();
        //final Serde<Long> longSerde = Serdes.Long();


        //final Serde<String> stringSerde = Serdes.String();
        //final Serde<Long> longSerde = Serdes.Long();
        
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", "http://localhost:8081");
        
        final KafkaProducer producer = new KafkaProducer<String, GenericRecord>(producerProps);



        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        
        final InputStream top_upsells_schemaInputStream = StreamingJoin_CartCountsAndInventoryTopics.class.getClassLoader()
            .getResourceAsStream("avro/com/pattersonconsultingtn/kafka/examples/streams/topcartupsells.avsc");

        final Schema top_cart_upsells_schema = new Schema.Parser().parse( top_upsells_schemaInputStream );
        
      final StreamsBuilder builder = new StreamsBuilder();
      
    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                                                                     "http://localhost:8081");

    final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

    final Serde<String> stringSerde = Serdes.String();
    
    
    
    // pull in inventory from Kafka topic
    final KStream<String, GenericRecord> mysql_tables_jdbc_inventory_kstream = builder.stream("mysql_tables_jdbc_inventory", Consumed.with( Serdes.String(), valueGenericAvroSerde ) );
    
    // rekey on item name (as inventory from MySQL has null key) and send rekeyed inventory to kafka topic to be read into KTable
    final KStream<String, GenericRecord> mysql_tables_jdbc_inventory_kstream_keyedOnObject = mysql_tables_jdbc_inventory_kstream.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
    	@Override
    	public KeyValue<String, GenericRecord> apply(final String null_key, final GenericRecord record) {
    		String newKey = record.get("name").toString();
    	    //send rekeyed inventory to kafka topic
    	    ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>("inventory_rekeyed", newKey, record);
    		producer.send(data);
    	    return new KeyValue<>(newKey, record );
    		}
    	});
    
    // create new KTable from rekeyed inventory topic
    final KTable<String, GenericRecord> rekeyed_inventory_ktable = builder.table("inventory_rekeyed", Consumed.with(Serdes.String(), valueGenericAvroSerde ) );
    
        
        // Create a stream of object detection events from the detected_cv_objects_avro topic, where the key of
        // a record is assumed to be the item name and the value an Avro GenericRecord
        // that represents the full details of the object detected in an image.
        final KStream<String, GenericRecord> detectedObjectsKStream = builder.stream(sourceTopicName);
                
        // join with inventory ktable
        final KStream<String, GenericRecord> enrichedCartObjects = detectedObjectsKStream
        		.join( rekeyed_inventory_ktable, new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>() {
        	@Override
        	public GenericRecord apply(GenericRecord cartObject, GenericRecord inventoryRecord) {
          
        		String cart = cartObject.get("class_name").toString();
        		String inventory = inventoryRecord.get("name").toString();
        		String upsell = inventoryRecord.get("upsell_item").toString();

        		// this is where we have to create a new avro schema for the output intermediate record

        		final GenericRecord joinedView = new GenericData.Record( top_cart_upsells_schema );
        		joinedView.put("item_name", cart );
        		joinedView.put("upsell_item_name", upsell );
        		joinedView.put("item_cart_count", 1L );
        		return joinedView;
        		
        	}
        	
        });
        
        // create TimeWindow for windowing operation (here it has been set to a minute with a grace period of 0 seconds)
        TimeWindows window = TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(0));
        
        // create windowed KTable that groups items by key and counts based on a 1-minute window
        // .suppress is needed so that only the final results of a window are sent out
      final KTable<Windowed<String>, GenericRecord> joinedKTable = enrichedCartObjects
    		  .groupByKey(Grouped.with(Serdes.String(), valueGenericAvroSerde))
    		  .windowedBy(window)
    		  .reduce(new Reducer<GenericRecord>() {
    			  @Override
    			  public GenericRecord apply(GenericRecord aggValue, GenericRecord newValue) {
    		
    				  Long lCount = Long.valueOf(aggValue.get("item_cart_count").toString());
    				  Long rCount = Long.valueOf(newValue.get("item_cart_count").toString());
    				  Long total = lCount + rCount;
    		    		
    				  aggValue.put("item_cart_count", total);
    				  
    				  return aggValue;
    			  }
    		  })
    		  .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
      
      // KStream used to output data to System.out
      final KStream<Windowed<String>, GenericRecord> outputStream = joinedKTable.toStream()
        .peek(new ForeachAction<Windowed<String>, GenericRecord>() {
        	@Override
        	public void apply(Windowed<String> key, GenericRecord value) {
      		  // outputting to System.out here
			  // optionally, this could be done through outputting to a Kafka topic
			  // in the end, all we need is:
			  // 		current time
			  //		how many of each item are in carts in the store
			  //		the upsell items for the items in carts
			  LocalDateTime now = LocalDateTime.now();
			  String item = value.get("item_name").toString();
			  String count = value.get("item_cart_count").toString();
			  String upsell = value.get("upsell_item_name").toString();
			  
			  // longest string is "tennis racket" at 13 characters
			  // set spacing to 15 characters
			  int diff = 15 - item.length();
			  
			  // determine space after item so counts line up
			  String space = "";
			  for(int i = 0; i < diff; i++) {
				  space += " ";
			  }
			  
			  System.out.println(dtf.format(now) + "\t" + "item: " + item + space + "count: " + count + "\t" + "upsell: " + upsell);
			}
        });

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
 
        // ... same as Pipe.java above
        Runtime.getRuntime().addShutdownHook(new Thread("GreenLightSpecial-Join-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });   


        try {
            System.out.println( "Starting JOIN Streaming App..." );

            // debug reset code!
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        
        producer.close();

        System.exit(0);

    }
}
