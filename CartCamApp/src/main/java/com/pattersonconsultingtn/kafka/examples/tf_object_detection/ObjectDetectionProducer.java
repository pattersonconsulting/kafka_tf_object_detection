package com.pattersonconsultingtn.kafka.examples.tf_object_detection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import java.util.List;
import java.util.Map;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;


import com.pattersonconsultingtn.kafka.examples.tf_object_detection.vision.TFVision_ObjectDetection;
import com.pattersonconsultingtn.kafka.examples.tf_object_detection.vision.VisualObject;

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
  -Dexec.args="10 http://localhost:8081 /tmp/"



// (6) check topic for entries
./bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic shopping_cart_objects --from-beginning








/Users/josh/Downloads/faster_rcnn_resnet101_coco_2018_01_28/saved_model/

mvn exec:java -Dexec.mainClass="com.pattersonconsultingtn.kafka.examples.tf_object_detection.ObjectDetectionProducer" \
  -Dexec.args="/Users/josh/Downloads/faster_rcnn_resnet101_coco_2018_01_28/saved_model/"



*/
public class ObjectDetectionProducer {

  private Producer<String, GenericRecord> producer = null;
  private Schema schema = null;
  private final String topicName = "shopping_cart_objects";
  private final String topicKey = "camera_0"; // sensorID
  private boolean sendEventsToKafka = false;
  private String schemaRegistryUrl = "";

  private final String labelFilePath = "./src/main/resources/";
  private final static String testImagesFilePath = "./src/main/resources/cart_images/";

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

    String modelFilePath = "";
    if (args.length > 0 ) {
      modelFilePath = args[0];
    } else {

      System.out.println("Please provide command line arguments: modelFilePath [schemaRegistryUrl:optional]");
      System.exit(-1);

    }


    String kafkaSchemaURL = "";
    if (args.length > 1) {
      kafkaSchemaURL = args[1];
    }



    ObjectDetectionProducer cv_producer = new ObjectDetectionProducer();
    cv_producer.run( kafkaSchemaURL, modelFilePath, testImagesFilePath );

  }

  public void run(String url, String modelFilePath, String input_folder) {


    if (url != "") {
      // System.out.println("Please provide command line arguments: schemaRegistryUrl");
      // System.exit(-1);
      schemaRegistryUrl = url;
      sendEventsToKafka = true;
      System.out.println( "Sending detected objects to Kafka topic: " + topicKey );


      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
      props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
      props.put("schema.registry.url", url);

      producer = new KafkaProducer<String, GenericRecord>(props);

      Schema.Parser parser = new Schema.Parser();
      schema = parser.parse( schemaString );


    } else {

      System.out.println( "### Not sending Events to Kafka... ");

      schemaRegistryUrl = "";

      sendEventsToKafka = false;

    }


    File f = new File( input_folder ); //).toPath()
    System.out.println( "Scanning: " + f.toPath() );

    try {
      scanAllFilesInDirectory( f, modelFilePath );
    } catch (Exception e) {
      System.out.println( e );
    }

    // Optionally, we could watch a path and send new data when it arrives...
    // watchDirectoryPath( f.toPath() );

    System.out.println( "closing producer..." );
    producer.close();
    System.out.println( "done..." );

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


      System.out.println( "Sending (" + className + ") avro object detection data for: " + imgName );

      // ##############                                                                      topic, key, value
      ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>( topicName, topicKey, detected_object_record );
     producer.send(data);


  }

  public void scanAllFilesInDirectory(File file, String modelFilePath) throws Exception, IOException {

    String labelMapFile = labelFilePath + "mscoco_label_map.pbtxt.txt";

    System.out.println( "\nmodel file: " + modelFilePath );
    System.out.println( "labels file: " + labelMapFile );

    TFVision_ObjectDetection imageScanner = new TFVision_ObjectDetection();


    System.out.println( "Scanning all existing files in: " + file + "\n" );


    File[] files = file.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
          return name.toLowerCase().endsWith(".jpg");
      }
    });

    for (final File fileEntry : files ) { //file.listFiles()) {

        if (fileEntry.isDirectory()) {

            // we are not scanning sub-directories in this example...

        } else {

          System.out.println( "scanning for objects: " + fileEntry.getAbsolutePath() );

          List<VisualObject> foundObjs = imageScanner.scanImageForObjects( modelFilePath, labelMapFile, fileEntry.getAbsolutePath() );

          for ( int x = 0; x < foundObjs.size(); x++ ) {

            // get prediction
            int classID = foundObjs.get( x ).getPredictedClass();
            String className = foundObjs.get( x ).label;
            float score = (float)foundObjs.get( x ).getConfidence();
            int box_x = foundObjs.get( x ).getLeft();
            int box_y = foundObjs.get( x ).getTop();
            int box_w = foundObjs.get( x ).getWidth();
            int box_h = foundObjs.get( x ).getHeight();

            System.out.println( "Debug - Sending: " + className );

            if (sendEventsToKafka) {

              sendDetectedObjectToKakfa( fileEntry.getName(), classID, className, score, box_x, box_y, box_w, box_h );

            }

          } // for

        }
    }

  }


  public void watchDirectoryPath(Path path) {
        // Sanity check - Check if path is a folder
        /*
        try {
            Boolean isFolder = (Boolean) Files.getAttribute(path,
                    "basic:isDirectory", NOFOLLOW_LINKS);
            if (!isFolder) {
                throw new IllegalArgumentException("Path: " + path
                        + " is not a folder");
            }
        } catch (IOException ioe) {
            // Folder does not exists
            ioe.printStackTrace();
        }
        */

        System.out.println("Watching path: " + path);

        // We obtain the file system of the Path
        FileSystem fs = path.getFileSystem();

        // We create the new WatchService using the new try() block
        try (WatchService service = fs.newWatchService()) {

            // We register the path to the service
            // We watch for creation events
            path.register(service, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            // Start the infinite polling loop
            WatchKey key = null;
            while (true) {
                key = service.take();

                // Dequeueing events
                Kind<?> kind = null;
                for (WatchEvent<?> watchEvent : key.pollEvents()) {
                    // Get the type of the event
                    kind = watchEvent.kind();
                    if (OVERFLOW == kind) {
                        continue; // loop
                    } else if (ENTRY_CREATE == kind) {
                        // A new Path was created
                        Path newPath = ((WatchEvent<Path>) watchEvent)
                                .context();
                        // Output
                        System.out.println("New path created: " + newPath);
                    } else if (ENTRY_MODIFY == kind) {
                        // modified
                        Path newPath = ((WatchEvent<Path>) watchEvent)
                                .context();
                        // Output
                        System.out.println("New path modified: " + newPath);
                    }
                }

                if (!key.reset()) {
                  System.out.println("break...");
                    break; // loop
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

    }





}
