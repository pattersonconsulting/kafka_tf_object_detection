# kafka_tf_object_detection
Detecting Computer Vision Objects in Images in Real-Time with Tensorflow and Apache Kafka

Associated Blog Post: http://www.pattersonconsultingtn.com/

## Running the Green Light Special Example

(1) Start Zookeeper. Since this is a long-running service, you should run it in its own terminal.
`
  ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties
`
(2) Start Kafka, also in its own terminal.
`
  ./bin/kafka-server-start ./etc/kafka/server.properties
`

(3) Start the Schema Registry, also in its own terminal.
`
./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties
`

(4) Create topic in Kafka
`
  ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 \
                    --partitions 1 --topic shopping_cart_objects
`

(5) Run the producer from maven
`
  mvn exec:java -Dexec.mainClass="com.pattersonconsultingtn.kafka.examples.tf_object_detection.ObjectDetectionProducer" \
   -Dexec.args="10 http://localhost:8081 /tmp/"
`

(6) check topic for entries
`
  ./bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic detected_cv_objects --from-beginning
`
