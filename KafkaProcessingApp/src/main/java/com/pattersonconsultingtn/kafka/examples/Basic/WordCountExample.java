package com.pattersonconsultingtn.kafka.examples.Basic;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

 
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
 
 
 /**


	Updates:

	(1) topic creation for input

bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-plaintext-input


	(2) topic creation for output

bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-wordcount-output \
    --config cleanup.policy=compact

    (3) Start the Streaming App

        mvn exec:java -Dexec.mainClass=com.pattersonconsultingtn.kafka.examples.WordCountExample

	(4) kafka producer setup from console

	bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input

	(5) kafka consumer setup from console

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

 */
public class WordCountExample {
 
    public static void main(String[] args) throws Exception {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //final Serde<String> stringSerde = Serdes.String();
        //final Serde<Long> longSerde = Serdes.Long();
 
        final StreamsBuilder builder = new StreamsBuilder();
 
        KStream<String, String> source = builder.stream("streams-plaintext-input");

        KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.split("\\W+"));
            }
        }).groupBy(new KeyValueMapper<String, String, String>() {
                           @Override
                           public String apply(String key, String value) {
                               return value;
                           }
                        })        
              .count(); //Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
              
              counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
 
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
 
        // ... same as Pipe.java above
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });   


        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}