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
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // init KafkaProducer
            KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

            int idx = 0;
            while(true){
                // set ProducerRecord
                String topic = "test";           // topic name
                String data = "record-"+ idx++;  // data
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);

                // send record (per second)
                producer.send(record);
                System.out.println("producer.send() >> [topic:" + topic + "][data:" + data + "]");
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
