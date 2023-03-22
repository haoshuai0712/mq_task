package com.demo.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.CountDownLatch;

public class TaskLauncher {
    private static final Logger logger = LoggerFactory.getLogger(TaskLauncher.class);
    public static void main(String[] args) throws InterruptedException {

        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("spring/spring-*.xml");
        logger.info(ctx.toString());
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Task Main Running!");
        latch.await();
    }
}
