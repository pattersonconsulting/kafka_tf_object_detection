package com.pattersonconsultingtn.kafka.examples.tf_object_detection;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

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
import org.apache.kafka.streams.Consumed;
 
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.InputStream;
import java.util.*; 
 
 
 /**

    #### 

    Load a KTable for the result topic of the application

    Use the Interactive Queries APIs (e.g., KafkaStreams.store(String, QueryableStoreType) 
    followed by ReadOnlyKeyValueStore.all()) to iterate over the keys of a KTable. 

    Alternatively convert to a KStream using toStream() and then use print(Printed.toSysOut()) on the result.    

 */
public class SalesFloor_UpsellView {
 
    public static void main(String[] args) throws Exception {
        
        // Streams properties ----- 
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pct-sales-floor-view");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "pct-sales-floor-view-client");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
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



        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        
        final InputStream top_upsells_schemaInputStream = StreamingJoin_CartCountsAndInventoryTopics.class.getClassLoader()
            .getResourceAsStream("avro/com/pattersonconsultingtn/kafka/examples/streams/topcartupsells.avsc");

        final Schema top_cart_upsells_schema = new Schema.Parser().parse( top_upsells_schemaInputStream );

        System.out.println( "Loaded Schema: " + top_cart_upsells_schema );



 
        final StreamsBuilder builder = new StreamsBuilder();
 


        // key is object name (String), value is the count (Long) in all baskets
        final KTable<String, Long> aggregate_cart_objects_ktable = builder.table("aggregate_cart_objects", Consumed.with( Serdes.String(), Serdes.Long() ) );
/*

        KStream<String, Long> unmodifiedStream2 = aggregate_cart_objects_ktable.toStream().peek(
            new ForeachAction<String, Long>() {
              @Override
              public void apply(String key, Long value) {
                System.out.println("key=" + key + ", value=" + value);
              }
            });        
*/


    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                                                                     "http://localhost:8081");

    final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

    final Serde<String> stringSerde = Serdes.String();

/*
        // key is null, record is record from MySQL databse table 'inventory'
        // this table needs to be re-keyed to do the join
        final KTable<String, GenericRecord> mysql_tables_jdbc_inventory_ktable = builder.table("mysql_tables_jdbc_inventory", Consumed.with( Serdes.String(), valueGenericAvroSerde ) );

        final KStream<String, GenericRecord> mysql_tables_jdbc_inventory_kstream_keyedOnObject = mysql_tables_jdbc_inventory_ktable.toStream().map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
              @Override
              public KeyValue<String, GenericRecord> apply(final String null_key, final GenericRecord record) {

                System.out.println( "Inventory > Re-Key: " + record.get("name").toString() );

                return new KeyValue<>(record.get("name").toString(), record );
              }
            });
*/

        // key is null, record is record from MySQL databse table 'inventory'
        // this table needs to be re-keyed to do the join
        final KStream<String, GenericRecord> mysql_tables_jdbc_inventory_kStream = builder.stream("mysql_tables_jdbc_inventory", Consumed.with( Serdes.String(), valueGenericAvroSerde ) );

/*
        KStream<String, GenericRecord> unmodifiedStream = mysql_tables_jdbc_inventory_kStream.peek(
            new ForeachAction<String, GenericRecord>() {
              @Override
              public void apply(String key, GenericRecord value) {
                System.out.println("Inventory Table --- key=" + key + ", value=" + value);
              }
            });        
*/


        final KStream<String, GenericRecord> mysql_tables_jdbc_inventory_kstream_keyedOnObject = mysql_tables_jdbc_inventory_kStream.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
              @Override
              public KeyValue<String, GenericRecord> apply(final String null_key, final GenericRecord record) {

                return new KeyValue<>(record.get("name").toString(), record );
              }
            });



        // https://stackoverflow.com/questions/49841008/kafka-streams-how-to-set-a-new-key-for-ktable
        // Because a key must be unique for a KTable (in contrast to a KStream) it's required to specify an aggregation function that aggregates all records with same (new) key into a single value.
        // KTable rekeyed = KTable.groupBy( .. ).aggregate( ... )





        /**
            The join between the re-keyed inventory data and the aggregate cart objects table

            We're using GenericRecord as the output message type, but the Avro schema is different for this record so we have to provide the schema

        */
        KStream<String, GenericRecord> tempAggCountsWithInventoryStream = mysql_tables_jdbc_inventory_kstream_keyedOnObject.join( aggregate_cart_objects_ktable,
            new ValueJoiner<GenericRecord, Long, GenericRecord>() {
              @Override
              public GenericRecord apply(GenericRecord inventoryRecord, Long aggregateCartCountForObject) {
                
                // thisis where we have to create a new avro schema for the output intermediate record

                //System.out.println( "Join: " + inventoryRecord.get("name") + " => " + aggregateCartCountForObject );

                    final GenericRecord joinedView = new GenericData.Record( top_cart_upsells_schema );
                    joinedView.put("item_name", inventoryRecord.get("name") );
                    joinedView.put("upsell_item_name", inventoryRecord.get("upsell_item") );
                    joinedView.put("item_cart_count", aggregateCartCountForObject );
                    return joinedView;


              }
            });   


        KStream<String, GenericRecord> unmodifiedStream_tempAggCountsWithInventoryStream = tempAggCountsWithInventoryStream.peek(
            new ForeachAction<String, GenericRecord>() {
              @Override
              public void apply(String key, GenericRecord value) {
                System.out.println("Join >>> key=" + key + ", value=" + value);
              }
            });        

                 


        KGroupedStream<String, GenericRecord> aggCountsWithInventoryGroupedStream = tempAggCountsWithInventoryStream.groupByKey();
        
        KTable<String, GenericRecord> joinedKTable = aggCountsWithInventoryGroupedStream.reduce(
            new Reducer<GenericRecord>() {
               public GenericRecord apply(GenericRecord left, GenericRecord right) {
                // since there should be no duplicates, just take the one that is not null

                System.out.println( "Reduce >> " + (String)( left.get("item_name").toString() ) );

                if (null != left && null != right) {

                    int left_count = Integer.valueOf( (String)(left.get("item_cart_count").toString()) );
                    int right_count = Integer.valueOf( (String)(right.get("item_cart_count").toString()) );

                    if (left_count > right_count) {
                        return left;
                    } else {
                        return right;
                    }

                }


                if (null != left) {
                 return left;
                }

                // else, return the other one
                return right;
               }
             }

            );


        KStream<String, GenericRecord> unmodifiedStreamJoinedKTable = joinedKTable.toStream().peek(
            new ForeachAction<String, GenericRecord>() {
              @Override
              public void apply(String key, GenericRecord value) {
                System.out.println("Joined KTable >>> key=" + key + ", value=" + value);
              }
            });        

        
        // stream.to(outputTopic, Produced.with(stringSerde, genericAvroSerde));
              
        joinedKTable.toStream().to( "top_cart_upsells", Produced.with( stringSerde, valueGenericAvroSerde ) );
 




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
  //          streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);

    }
}