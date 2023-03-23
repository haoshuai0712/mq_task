package com.demo.example;

public class StarterTestServiceImpl implements StarterTestService {

    private ExampleProperties exampleProperties;

    public StarterTestServiceImpl(ExampleProperties exampleProperties) {
        this.exampleProperties = exampleProperties;
    }

    @Override
    public void hello(String name) {
        System.out.println(name + " get exampleProperties: " + exampleProperties.getName());
    }
}