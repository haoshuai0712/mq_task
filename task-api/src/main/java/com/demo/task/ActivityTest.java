package com.demo.task;

public abstract class ActivityTest {

    /**
     * 验证
     *
     * @throws Exception
     */
    protected void validate() throws Exception {

    }

    /**
     * 启动
     * @throws Exception
     */
    protected void start() throws Exception {
        validate();
        doStart();
    }

    /**
     * 启动
     *
     * @throws Exception
     */
    protected void doStart() throws Exception {
        System.out.println("ActivityTest doStart.");
    }
}
