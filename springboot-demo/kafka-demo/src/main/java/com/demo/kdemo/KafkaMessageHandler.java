package com.demo.kdemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class KafkaMessageHandler implements Runnable {
    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public KafkaMessageHandler(KafkaConsumer<String, String> consumer, CountDownLatch latch) {
        this.consumer = consumer;
        this.latch = latch;
    }

    @Override
    public void run() {
        // 异步处理消息
        // 减少计数器值
        latch.countDown();
        // ...
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("Thread %s get record, key: %s, value: %s, offset: %s", Thread.currentThread().getId(), record.key(), record.value(), record.offset()));
            }
        }
    }
}
