package com.github.simplesteph.kafka.tutorial;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world " + i);


            // send data - asynchronous
            producer.send(producerRecord, (recordMetadata, e) -> {
                // executes everytime record is successfully sent or exception is thrown
                if (e == null) {
                    // the record is successfully sent
                    log.info("New Metadata received");
                    log.info("Topic: " + recordMetadata.topic());
                    log.info("Partition: " + recordMetadata.partition());
                    log.info("Offset: " + recordMetadata.offset());
                    log.info("Timestamp: " + recordMetadata.timestamp());

                } else {

                    log.error("Error while producing", e);

                }
            });
            producer.flush();
        }
        producer.close();

    }
}
