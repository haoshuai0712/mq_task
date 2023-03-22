package com.demo.task;

public abstract class ServiceTest extends ActivityTest {

    @Override
    protected void validate() throws Exception {
        super.validate();
        System.out.println("ActivityTest validate.");
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

}
