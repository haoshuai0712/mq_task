package com.demo.task.model;

import com.jd.joyqueue.model.domain.Task;
import com.jd.laf.extension.Type;
import org.joyqueue.toolkit.lang.LifeCycle;

public interface TaskExecutor extends Type, LifeCycle {

    /**
     * 设置并执行任务
     *
     * @param context 上下文
     * @throws Exception
     */
    void execute(TaskContext context) throws Exception;

    /**
     * 返回当前执行器执行的任务
     *
     * @return 当前执行器执行的任务
     */
    Task getTask();

    /**
     * 设置执行任务
     *
     * @param task
     */
    void setTask(Task task);

    /**
     * 强制关闭
     */
    void stop();

    /**
     * 是否启动
     *
     * @return 启动标示
     */
    boolean isStarted();

    void onSuccess();

    void onException(Exception e);


}
