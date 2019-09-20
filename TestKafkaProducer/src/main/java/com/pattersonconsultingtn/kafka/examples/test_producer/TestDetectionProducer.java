package com.pattersonconsultingtn.kafka.examples.test_producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**

Quick Start

# (1) Start Zookeeper. Since this is a long-running service, you should run it in its own terminal.
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

# (2) Start Kafka, also in its own terminal.
$ ./bin/kafka-server-start ./etc/kafka/server.properties

# (3) Start the Schema Registry, also in its own terminal.
./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties



// (4) Create topic in Kafka

./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 \
                   --partitions 1 --topic detected_cv_objects


// (5) Run the producer from maven

mvn exec:java -Dexec.mainClass="com.pattersonconsultingtn.kafka.examples.tf_object_detection.ObjectDetectionProducer" \
  -Dexec.args="./src/main/resources/cart_images/"



// (6) check topic for entries
./bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic shopping_cart_objects --from-beginning



*/
public class TestDetectionProducer {
	
	private static Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
	private Producer<String, GenericRecord> producer = null;
	private Schema schema = null;
	private final String topicName = "shopping_cart_objects";
  private final String schemaString = "{\"namespace\": \"com.pattersonconsulting.kafka.avro\", " +
                            "\"type\": \"record\", " +
                           "\"name\": \"" + topicName + "\"," +
                           "\"fields\": [" +
                            "{\"name\": \"timestamp\", \"type\": \"long\"}," +
                            "{\"name\" : \"image_name\", \"type\" : \"string\", \"default\" : \"NONE\"}, " +
                            "{\"name\": \"class_id\", \"type\": \"int\", \"default\":-1 }," +
                            "{\"name\" : \"class_name\", \"type\" : \"string\", \"default\" : \"NONE\"}, " +
                            "{\"name\": \"score\", \"type\": \"float\", \"default\":0.0 }," +

                            "{\"name\": \"box_x\", \"type\": \"int\", \"default\":-1 }," +
                            "{\"name\": \"box_y\", \"type\": \"int\", \"default\":-1 }," +
                            "{\"name\": \"box_w\", \"type\": \"int\", \"default\":-1 }," +
                            "{\"name\": \"box_h\", \"type\": \"int\", \"default\":-1 }" +

                           "]}";


  public static void main(String[] args){
    
	root.setLevel(Level.ERROR);

    TestDetectionProducer cv_producer = new TestDetectionProducer();
    
    cv_producer.runWithFakeData();
    
  }
  
  public void runWithFakeData() {


	    Properties props = new Properties();
	    props.put("bootstrap.servers", "localhost:9092");
	    props.put("acks", "all");
	    props.put("retries", 0);
	    props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
	    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
	    props.put("schema.registry.url", "http://localhost:8081");

	    Schema.Parser parser = new Schema.Parser();
	    schema = parser.parse( schemaString );
	    
	    while(true){
		    producer = new KafkaProducer<String, GenericRecord>(props);
	    	
	    	// random number to generate fake data
	    	Random rand = new Random();
	    	int num = rand.nextInt(7);
	    	String item;
	    	
	    	switch(num) {
	    	case 0: item = "cup";
	    	break;
	    	case 1: item = "bowl";
	    	break;
	    	case 2: item = "fork";
	    	break;
	    	case 3: item = "spoon";
	    	break;
	    	case 4: item = "sportsball";
	    	break;
	    	case 5: item = "tennis racket";
	    	break;
	    	case 6: item = "frisbees";
	    	break;
	    	default: item = "unknown";
	    	break;
	    	}
	    	
	    	sendDetectedObjectToKakfa(item, num, item, 99, 1, 2, 3, 4);
	    	
	        System.out.println( "closing producer..." );
	        producer.close();
	        System.out.println( "done..." );
	    	
	      try{
	       Thread.sleep(15000);
	      }
	      catch(InterruptedException ex){
	       Thread.currentThread().interrupt();
	      }
	      
	    }

  }

  

  public void sendDetectedObjectToKakfa( String imgName, int classID, String className, float score, int box_x, int box_y, int box_w, int box_h ) {

      GenericRecord detected_object_record = new GenericData.Record( schema );

      long runtime = new Date().getTime();
      detected_object_record.put("timestamp", runtime);
      detected_object_record.put("image_name", imgName );
      detected_object_record.put("class_id", classID );
      detected_object_record.put("class_name", className );
      detected_object_record.put("score", score );

      detected_object_record.put("box_x", box_x );
      detected_object_record.put("box_y", box_y );
      detected_object_record.put("box_w", box_w );
      detected_object_record.put("box_h", box_h );


      System.out.println( "Sending avro object detection data for: " + imgName );

      // ##############                                                                      topic, key, value
      ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>( topicName, className, detected_object_record );
     producer.send(data);


  }






}
