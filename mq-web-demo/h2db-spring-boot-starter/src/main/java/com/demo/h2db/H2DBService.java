package com.demo.h2db;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(H2DBServerAutoConfiguration.class)
public @interface H2DBService {
    boolean isOn() default true;
}
