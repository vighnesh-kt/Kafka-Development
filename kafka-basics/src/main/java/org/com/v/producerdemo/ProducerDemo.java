package org.com.v.producerdemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger= LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {

        logger.info("I am a Kafka Producer");

        //create producer properties

        //properties tell how to connect to kafka and how to serialize data

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:19092");

        //set producer properties

//        to serialize data using kafka provided serializer before sending to kafka
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //Create a producer record which is sned to kafka

        ProducerRecord<String,String> producerRecord= new ProducerRecord<>("demo_java","hello world");

        //send data

        producer.send(producerRecord);


        //flush and close produccer
        //tell producer to send all data and block until done --synchronous

        producer.flush();

        // close will also call flush before doing it
        producer.close();
    }
}
