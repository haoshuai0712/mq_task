package com.demo.task;


import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public class TaskBeanTest extends ServiceTest {

    private String prop;

    @Override
    protected void validate() throws Exception {
        super.validate();
        System.out.println("TaskBeanTest validate.");
    }

    @PostConstruct
    public void init() {
        System.out.println("TaskBeanTest init, prop: " + prop);
    }

    @Override
    protected void doStart() throws Exception {
        System.out.println("TaskBeanTest doStart.");
        super.doStart();
    }

    public TaskBeanTest() {
    }

    @PreDestroy
    public void destroy() {
        System.out.println("TaskBeanTest destroy.");
    }

    public String getProp() {
        return prop;
    }

    public void setProp(String prop) {
        this.prop = prop;
    }
}
