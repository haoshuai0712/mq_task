package com.demo.example.test;

import com.demo.h2db.H2DBServerAutoConfiguration;
import com.demo.h2db.H2DBService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource({"classpath:application.properties"})
//@H2DBService(isOn = true)
@Import(H2DBServerAutoConfiguration.class)
public class ExampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class);
    }
}
