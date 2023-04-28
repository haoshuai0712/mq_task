package com.demo.example.test;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerDemo {
    @KafkaListener(topics = "jmqq", concurrency = "2")
    public void consume(String message) {
        System.out.println("Received message: " + message);
    }
}
