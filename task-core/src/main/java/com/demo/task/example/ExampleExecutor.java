package com.demo.task.example;

import com.demo.task.model.AbstractTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleExecutor extends AbstractTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ExampleExecutor.class);

    @Override
    public Object type() {
        return "example";
    }

    @Override
    public void onException(Exception e) {
        super.onException(e);
        logger.error("ExampleExecutor exception: {}", e.getMessage());
    }

    @Override
    protected void doExecute() throws Exception {
        super.doExecute();
        logger.info("ExampleExecutor doExecute.");
    }
}
