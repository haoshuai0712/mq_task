package com.demo.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource({"classpath:application.properties"})
public class MqWebApplication implements ApplicationRunner {
    private static final Logger logger = LoggerFactory.getLogger(MqWebApplication.class);

    @Value("${vertx.http.port}")
    private int port ;
    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name","io.vertx.core.logging.SLF4JLogDelegateFactory");
        SpringApplication.run(MqWebApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("JoyQueue web started on port {}.", port);
    }
}
