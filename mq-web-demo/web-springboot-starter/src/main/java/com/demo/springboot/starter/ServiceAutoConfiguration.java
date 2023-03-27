package com.demo.springboot.starter;

//import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;
//import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Service auto configuration
 * Created by haoshuai1 on 23-03-16.
 */
@Configuration
//@ComponentScan(value = {
//        "org.joyqueue.context",
//        "org.joyqueue.service.impl",
//        "org.joyqueue.util",})
//@MapperScan(basePackages = {"org.joyqueue.repository"})
//@EnableTransactionManagement
@EnableAspectJAutoProxy(exposeProxy = true)
@EnableScheduling
public class ServiceAutoConfiguration {

}