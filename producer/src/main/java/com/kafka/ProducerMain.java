package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * packageName    : com.kafka
 * fileName       : ProducerMain
 * author         : Jihun Park
 * date           : 2022/04/04
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2022/04/04        Jihun Park       최초 생성
 */
public class ProducerMain {

    public static void main(String[] args) {

        try {
            // set kafka properties
            Properties configs = new Properties();
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  // kafka cluster
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());// KEY_SERIALIZER
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // VALUE_SERIALIZER

            // init KafkaProducer
            KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

            int idx = 0;
            while(true){

                // set ProducerRecord
                String topic = "test";           // topic name
                Integer partition = 0;               // partition number
                String key = "key-" + idx;       // key
                String data = "record-"+ idx;    // data
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);                  // ProducerRecord(@NotNull String topic, V value)
                ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, key, data);            // ProducerRecord(@NotNull String topic, K key, V value)
                ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, partition, key, data); // ProducerRecord(@NotNull String topic, Integer partition, K key, V value)

                // send record (per second)
                producer.send(record);

                System.out.println("producer.send() >> [topic:" + topic + "][data:" + data + "]");
                Thread.sleep(1000);
                idx++;
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
