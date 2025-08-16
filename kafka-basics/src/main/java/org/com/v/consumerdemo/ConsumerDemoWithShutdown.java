package org.com.v.consumerdemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger logger= LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main(String[] args) {

        logger.info("I am a Kafka Consumer");
        String groupId="my-java-application";
        String topic="demo_java";


        //create consumer properties

        //properties tell how to connect to kafka and how to serialize data

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:19092");

        //set producer properties

//        to serialize data using kafka provided serializer before sending to kafka
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create consumer config

        ///creating deserializer for consumer side based on the data here it is string so string deserializer
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");

        //Create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);

        //create shutdown hook

        //Get reference to the current thread
        final Thread t=Thread.currentThread();

        //adding shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                logger.info("Detected shutdown lets exit by calling consumer.wakeup()");
                consumer.wakeup();

                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        try {
            //Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //Poll for the data
            while (true) {
                logger.info("Consumer is Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key is " + record.key() + " Value is " + record.value());
                    logger.info("Partition is " + record.partition() + " Offset is " + record.offset());
                }
            }

        }catch (WakeupException e){
            logger.info("Consumer is shutting down");
        }catch (Exception e){
            logger.info("Unexpeced exception in the consumer",e);
        }finally {
            consumer.close(); //this will also commit offsets
            logger.info("The consumer is now gracefully shutdown");
        }



    }
}
