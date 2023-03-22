package com.demo.task.model;

import com.jd.joyqueue.model.domain.Task;
import org.joyqueue.toolkit.URL;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任务执行器的抽象类
 * <p>
 * 支持：支持任务重试、cron表达式
 * 任务过程中的io、cpu、memory 监控
 * 日志收集
 **/
public abstract class AbstractTaskExecutor implements TaskExecutor {
    protected URL url;
    protected Task task;
    protected TaskContext context;
    protected AtomicBoolean started = new AtomicBoolean(true);

    @Override
    public void execute(final TaskContext context) throws Exception {
        this.context = context;
        try {
            initialize(context);
            doExecute();
            clear();
            onSuccess();
        } catch (Exception e) {
            onException(e);
            stop();
            throw e;
        }
    }

    /**
     * 清除资源
     */
    protected void clear() {

    }

    /**
     * 初始化数据
     *
     * @param context
     * @throws Exception
     */
    protected void initialize(TaskContext context) throws Exception {

    }

    /**
     * 执行业务逻辑，需要较长时间
     *
     * @throws Exception
     */
    protected void doExecute() throws Exception {

    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public Task getTask() {
        return task;
    }

    @Override
    public void setTask(Task task) {
        this.task = task;
    }

    @Override
    public final void stop() {
        if (started.compareAndSet(true, false)) {
            clear();
        }
    }

    @Override
    public void onSuccess() {

    }

    @Override
    public void onException(Exception e) {

    }

    @Override
    public void start() throws Exception {

    }
}

