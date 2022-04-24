package com.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * packageName    : com.kafka
 * fileName       : ConsumerMain
 * author         : Jihun Park
 * date           : 2022/04/04
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2022/04/22        Jihun Park       최초 생성
 */
public class ConsumerMain {

    public static void main(String[] args) {

        try {
            // set kafka properties
            Properties configs = new Properties();
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  // kafka cluster
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());// KEY_SERIALIZER
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // VALUE_SERIALIZER

            // init KafkaConsumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
