package com.demo.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(value = ExampleProperties.class)
public class ExampleAutoConfiguration {

    @Autowired
    private ExampleProperties exampleProperties;

    @Bean
    public StarterTestService starterTestService() {
        return new StarterTestServiceImpl(exampleProperties);
    }

}
