package com.demo.task.model;

import org.joyqueue.toolkit.config.Context;

import java.util.Map;

public class TaskContext extends Context {

    public static final String TASK_LIVE_PATH = "task/live";
    public static final String TASK_LEADER_PATH = "task/leader";
    public static final String TASK_EXECUTOR_PATH = "task/executor";

    public TaskContext(Map<String, Object> parameters) {
        super(parameters);
    }

    public <T> T getParameter(String name, Class<T> clazz) {
        return clazz.cast(this.parameters.get(name));
    }

}
