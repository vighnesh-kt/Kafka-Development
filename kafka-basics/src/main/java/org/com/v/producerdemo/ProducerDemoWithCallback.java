package org.com.v.producerdemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        logger.info("I am a Kafka Producer with callbcack");

        //create producer properties

        //properties tell how to connect to kafka and how to serialize data

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:19092");

        //set producer properties

//        to serialize data using kafka provided serializer before sending to kafka
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","400");
//        properties.setProperty("partition.class", RoundRobinPartitioner.class.getName());

        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);


        //Create a producer record which is sned to kafka

        //producing multiple data


        for (int i=1;i<=5;i++){
            ProducerRecord<String,String> producerRecord= new ProducerRecord<>("demo_java","hello world"+i);

            //send data

//        producer.send(producerRecord);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 5. This is called when the broker responds
                    if (exception == null) {
                        // ✅ Success: print metadata
                        System.out.println("Message sent successfully!");
                        System.out.println("Topic: " + metadata.topic());
                        System.out.println("Partition: " + metadata.partition());
                        System.out.println("Offset: " + metadata.offset());
                        System.out.println("Timestamp: " + metadata.timestamp());
                    } else {
                        // ❌ Failure: print exception
                        exception.printStackTrace();
                    }
                }
            });

        }


        //flush and close produccer
        //tell producer to send all data and block until done --synchronous

        producer.flush();

        // close will also call flush before doing it
        producer.close();
    }
}
