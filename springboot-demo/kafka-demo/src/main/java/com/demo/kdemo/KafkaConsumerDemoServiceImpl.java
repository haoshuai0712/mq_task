package com.demo.kdemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Service("kafkaConsumerDemoService")
public class KafkaConsumerDemoServiceImpl {

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(4);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "jmq4drctestapp2");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "jmq4drctestapp2");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Collections.singletonList("jmqq"));
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            if (!records.isEmpty()) {
//                // 异步处理消息
//                for (ConsumerRecord<String, String> record: records) {
//                    System.out.println(record);
//                }
//            }
//        }

        int numConsumers = 3;
        for (int i = 0; i < numConsumers; i++) {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("jmqq"));
            System.out.println("KafkaMessageHandler start.");
            Thread consumerThread = new Thread(new KafkaMessageHandler(consumer, countDownLatch));
            consumerThread.start();
        }

    }
}
