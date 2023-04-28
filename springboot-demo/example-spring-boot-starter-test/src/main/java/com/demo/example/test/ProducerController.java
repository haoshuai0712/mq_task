package com.demo.example.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/send")
    public String sendMessage(@RequestBody String message) {
        kafkaTemplate.send("test-topic", message);
        return "Message sent successfully.";
    }
}
