package com.demo.task;

import com.google.common.base.Strings;
import com.jd.joyqueue.model.Exceptions;
import com.jd.joyqueue.model.TaskException;
import com.jd.joyqueue.model.domain.Task;
import com.jd.joyqueue.registry.Registry;
import com.jd.joyqueue.registry.listener.ConnectionEvent;
import com.jd.joyqueue.registry.listener.ConnectionListener;
import com.jd.joyqueue.registry.listener.PathEvent;
import com.jd.joyqueue.registry.listener.PathListener;
import com.jd.joyqueue.registry.util.Path;
import com.demo.task.dao.ConfigDao;
import com.demo.task.dao.TaskDao;
import com.demo.task.model.TaskContext;
import com.demo.task.model.TaskExecutor;
import org.joyqueue.domain.Config;
import org.joyqueue.model.exception.RepositoryException;
import org.joyqueue.toolkit.URL;
import org.joyqueue.toolkit.concurrent.EventBus;
import org.joyqueue.toolkit.concurrent.EventListener;
import org.joyqueue.toolkit.config.Context;
import org.joyqueue.toolkit.config.Postman;
import org.joyqueue.toolkit.config.PostmanUpdater;
import org.joyqueue.toolkit.network.IpUtil;
import org.joyqueue.toolkit.service.Service;
import org.joyqueue.toolkit.time.CronExpression;
import org.joyqueue.toolkit.time.DateTime;
import org.joyqueue.toolkit.validate.annotation.NotEmpty;
import org.joyqueue.toolkit.validate.annotation.NotNull;
import org.joyqueue.toolkit.validate.annotation.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static com.jd.joyqueue.model.domain.Task.Status.RETRY;

public class TaskJob extends Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskJob.class);
    protected static final String HISTORY = "history";
    protected static final String TASK = "task";

    // 注册中心
    @NotNull
    private Registry registry;
    // 任务DAO
    @NotNull
    private TaskDao taskDao;
    // 配置DAO
    private ConfigDao configDao;
    // 并发任务数量
    @Range(min = 1)
    private int concurrency = 20;
    // 外部初始化的参数
    protected Map<String, Object> parameters = new HashMap<String, Object>();
    // 应用名称
    @NotEmpty
    private String application;
    // 存活节点
    private String livePath;
    // 上下文构建器
    @NotNull
    private Postman postman;
    // 节点名称
    private String identity;
    // 任务计划节点
    private String schedulePath;
    // 线程池
    private ExecutorService service;
    private EventBus<Set<Long>> tasks = new EventBus<Set<Long>>();
    // 连接监听器
    private ConnectionListener connectionListener = new TaskConnectionListener();
    // 节点监听器
    private PathListener scheduleListener = new ScheduleListener();
    //当前在本节点中正在执行的任务
    private ConcurrentMap<Long, TaskExecutor> executors = new ConcurrentHashMap<Long, TaskExecutor>();
    // 任务监听器
    private TaskListener taskListener = new TaskListener();

    // 配置分组
    private String configGroup = "task";

    @Override
    protected void validate() throws Exception {
        super.validate();

        livePath = Path.concat(application, "/task/live");
        schedulePath = Path.concat(application, "/task/executor");
        Path.validate(livePath);
        Path.validate(schedulePath);
        identity = IpUtil.getLocalIp();
        Path.validate(Path.concat(livePath, identity));
        PostmanUpdater postman = new PostmanUpdater() {
            @Override
            protected Context update(String group) {
                return null;
            }
        };
        postman.setInterval(100);
        postman.start();
        this.postman = postman;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        logger.info("TaskJob doStart.");
        // 合并事件
        tasks.setInterval(1000L);
        tasks.addListener(taskListener);
        tasks.start();


        // 注册存活
        registry.createLive(Path.concat(livePath, identity), null);
        // 注册节点监听器
        registry.addListener(schedulePath, scheduleListener);
        // 注册链接监听器
        registry.addListener(connectionListener);

        logger.info(String.format("task node %s is started.", identity));
    }

    @Override
    protected void doStop() {
        super.doStop();
    }

    /**
     * 启动任务监听器
     */
    protected class TaskListener implements EventListener<Set<Long>> {
        @Override
        public void onEvent(Set<Long> event) {
            logger.info("TaskJob TaskListener onEvent Running...");
            if (!isStarted()) {
                return;
            }
            // 单线程执行，加锁防止在停止操作
            writeLock.lock();
            try {
                if (!isStarted()) {
                    return;
                }
                if (logger.isInfoEnabled()) {
                    logger.info("tasks on {} is changed to {}", identity, event.toString());
                }
                launch(event);
            } catch (Exception e) {
                logger.error("launch task error.", e);
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * 获取并执行已分配的任务
     *
     * @param tasks 新获取到的任务id 的集合
     * @throws Exception
     */
    protected void launch(Set<Long> tasks) throws Exception {
        if (tasks == null) {
            tasks = new HashSet<Long>();
        }
        for (Map.Entry<Long, TaskExecutor> entry : executors.entrySet()) {
            // 终止任务
            if (!tasks.contains(entry.getKey())) {
                //如果与zookeeper的连接断开，会生成一个空taskSet,表示要终止正在执行的任务
                TaskExecutor executor = entry.getValue();
                executor.stop();
                executors.remove(entry.getKey());
            }
        }

        Context context = postman.get(TASK);

        // 新增任务
        for (Long taskId : tasks) {
            // 过滤掉正在执行的任务
            if (executors.containsKey(taskId)) {
                continue;
            }
            try {
                // 重新获取任务，判断任务状态，防止被修改了
                launch(taskId, context);
            } catch (Exception e) {
                logger.error("start task error. ", e);
            }
        }

    }

    /**
     * 启动任务
     *
     * @param taskId  任务ID
     * @param context 上下文配置
     */
    protected void launch(final long taskId, final Context context) throws Exception {
        // 重新获取任务，判断任务状态，防止被修改了
        Task task = taskDao.findById(taskId);
        if (task == null) {
            // 任务不存在
            logger.info("task {} is not found. ", taskId);
            return;
        } else if (!identity.equals(task.getOwner())) {
            // 任务被分配到了其它节点
            logger.info("task {} is assigned to {}. ", task.getId(), task.getOwner());
            return;
        } else if (task.getStatus() != Task.DISPATCHED && task.getStatus() != Task.RUNNING) {
            // 任务任务状态不是可运行状态，则不启动
            logger.info("task {} status is changed. ", task.getId());
            return;
        }

        URL url = null;
        TaskExecutor executor = getService(task.getType());
        if (executor == null) {
            // 找不到任务执行器
            executors.remove(task.getId());
            String exception = "The executor type is not implement," + task.getType();
            if (task.isRetry() && (task.getMaxRetryCount() <= 0 || task.getRetryCount() < task.getMaxRetryCount())) {
                // 需要重试的任务，并且没有超过最大重试次数。因为没有执行器，2小时后重试
                task.setRetryTime(DateTime.of().addMillisecond(1000 * 60 * 60 * 2).date());
                task.setException(exception);
                task.setRetryCount(task.getRetryCount()+1);
                taskDao.update(task);
            } else {
                task.setException(exception);
                task.setStatus(Task.FAILED_NO_RETRY);
                taskDao.update(task);
            }
            logger.error(exception);
        } else {
            executor.setTask(task);
            executors.put(task.getId(), executor);
            service.submit(new Launcher(executor, url, context));
            if (logger.isDebugEnabled()) {
                logger.debug("execute task {}", task.getId());
            }
        }
    }

    private TaskExecutor getService(String type) {
        if (Strings.isNullOrEmpty(type)) {
            logger.warn("The task's type can not be null or empty!");
            return null;
        }
        Iterable<TaskExecutor> collectors = ServiceLoader.load(TaskExecutor.class);
        Iterator<TaskExecutor> it = collectors.iterator();
        while (it.hasNext()) {
            TaskExecutor executor = it.next();

            if (type.equalsIgnoreCase(executor.type().toString())) {
                return executor;
            }
        }
        logger.warn("Do not find the correct executor for task type {}", type);
        return null;
    }

    /**
     * 监听调度计划节点的数据变化<br>
     * 心跳感知，除了变化通知，会定时获取到当前的节点数据<br>
     * 可以避免只有重试任务，一致分配到某个节点，无法重新调度
     */
    protected class ScheduleListener implements PathListener, EventListener.Heartbeat {
        private long last;

        @Override
        public void onEvent(final PathEvent event) {
            logger.info("TaskJob ScheduleListener onEvent Running.");
            try {
                Set<Long> message = new HashSet<Long>();
                if (event.getData() != null) {
                    List<DispatchContext.NodeTask> nodes = DispatchContext.unmarshal(new String(event.getData(), StandardCharsets.UTF_8));
                    for (DispatchContext.NodeTask node : nodes) {
                        if (identity.equals(node.name)) {
                            for (Task task : node.tasks) {
                                message.add(task.getId());
                            }
                            break;
                        }
                    }
                }
                tasks.add(message);
            } catch (NumberFormatException e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public boolean trigger(final long now) {
            // 每10秒触发心跳
            if (now - last >= 10 * 1000) {
                last = now;
                return true;
            }
            return false;
        }
    }

    protected class TaskConnectionListener implements ConnectionListener {
        @Override
        public void onEvent(ConnectionEvent event) {
            logger.info("TaskJob TaskConnectionListener onEvent Running.");
            // zookeeper连接断开 关闭所有任务
            if (event.getType() == ConnectionEvent.ConnectionEventType.LOST) {
                tasks.add(new HashSet<Long>());
            }
        }
    }

    /**
     * 调用执行器
     */
    protected class Launcher implements Callable<Boolean> {
        private TaskExecutor executor;
        private Context context;
        private URL url;

        public Launcher(TaskExecutor executor, URL url, Context context) {
            this.executor = executor;
            this.url = url;
            this.context = context;
        }

        @Override
        public Boolean call() throws Exception {
            if (!isStarted()) {
                return Boolean.FALSE;
            }
            Task task = executor.getTask();
            // 保留原有重试时间
            Date oldRetryTime = task.getRetryTime();

            logger.info("start task {}", task.getId());
            try {
                // 开始执行任务，将状态设置为正在运行
                task.setStatus(Task.RUNNING);
                if (taskDao.update(task) == 1) {
                    List<Config> configs = null;
                    if (configGroup != null && !configGroup.isEmpty()) {
                        configs = configDao.findByGroup(configGroup);
                    }
                    task.setStatus(Task.RUNNING);
                    executor.execute(getTaskContext(task, configs));
                    // 执行任务,Daemon线程正常情况不要退出
                    executor.start();
                    // 任务执行完毕，将状态设置为成功
                    if (executor.isStarted() && !task.isDaemons()) {
                        // 计算下一个执行时间
                        Date retryTime = getNextTime(task.getCron());
                        if (retryTime == null) {
                            task.setStatus(Task.SUCCESSED);
                            taskDao.update(task);
                        } else {
                            task.setException("");
                            task.setStatus(Task.NEW);
                            task.setRetryCount(0);
                            task.setRetryTime(retryTime);
                            // 增加是否产生历史记录参数，频繁的调用，减少数据条数
                            taskDao.update(task);
                        }
                    }
                    logger.info("finish task {}", task.getId());
                    return Boolean.TRUE;
                } else {
                    logger.info("task {} status is changed. ", task.getId());
                }
            } catch (RepositoryException e) {
                // 更新数据库状态错误，会重新派发，要求任务防重入
                logger.error("update task error.", e);
            } catch (Throwable e) {
                TaskException te = null;
                if (e instanceof TaskException) {
                    te = (TaskException) e;
                }
                if (te != null && te.getAction() == TaskException.Action.STOP_NOW) {
                    // 任务被强制终止,可能已分配到其他机器执行
                    logger.warn(String.format("The task %d is stopped now.", task.getId()));
                } else {
                    if (te != null && te.getCause() == null) {
                        logger.error(String.format("execute task %d error. %s", task.getId(), e.getMessage()));
                    } else {
                        logger.error(String.format("execute task %d error.", task.getId()), e);
                    }
                    // 判断是否需要重试
                    Date retryTime = getNextTime(task.getCron());
                    if (retryTime == null) {
                        boolean retry = retryOnException(task, oldRetryTime, e);
                        // 异常信息
                        task.setException(e.getMessage());
                        taskDao.update(task);
                        // 不重试则尝试调用失败处理
                        if (!retry) {
                            executor.onException(new Exception(e));
                        }
                    } else {
                        task.setException(getException(e));
                        task.setStatus(Task.NEW);
                        task.setRetryCount(0);
                        task.setRetryTime(retryTime);
                        // 增加是否产生历史记录参数，频繁的调用，减少数据条数
                        taskDao.update(task);
                    }
                }
            } finally {
                executor.stop();
                executors.remove(task.getId());
            }
            return Boolean.FALSE;
        }

    }

    /**
     * 异常重试
     *
     * @param task         任务
     * @param oldRetryTime 执行之前的重试时间
     * @param e            异常
     * @return 是否重试
     */
    protected boolean retryOnException(final Task task, final Date oldRetryTime, final Throwable e) {
        boolean retry = true;
        TaskException te = null;
        if (e instanceof TaskException) {
            te = (TaskException) e;
        }
        // 周期任务，计算下一个执行时间
        Date retryTime = null;
        try {
            retryTime = getNextTime(task.getCron());
        } catch (Exception ex) {
            logger.error("executor get next retry time error", ex);
        }
        if ((task.isRetry() || te != null && (te.getAction() == TaskException.Action.RETRY || te
                .getAction() == TaskException.Action.RETRY_IMMUTABLE)) && (task.getMaxRetryCount() <= 0 || task
                .getRetryCount() < (task.getMaxRetryCount()))) {
            // 重试任务或重试异常，并且没有超过最大重试次数
            task.setStatus(RETRY.value());
            if (te == null || te.getAction() != TaskException.Action.RETRY_IMMUTABLE) {
                task.setRetryCount(task.getRetryCount() + 1);
            }
            if (task.getRetryTime() == null || (task.getRetryTime().equals(oldRetryTime))) {
                // 修改重试时间
                if (te != null && te.getDelay() > 0) {
                    task.setRetryTime(DateTime.of().addMillisecond(te.getDelay()).date());
                } else {
                    // 10秒钟以后重试
                    task.setRetryTime(DateTime.of().addMillisecond(TaskException.RETRY_INTERVAL).date());
                }
            }
        } else {
            // 不重试
            task.setStatus(Task.FAILED_NO_RETRY);
            retry = false;
        }

        return retry;
    }

    /**
     * 获取下一次执行时间
     *
     * @param cron 表达式
     * @return
     */
    protected Date getNextTime(final String cron) {
        if (cron == null || cron.isEmpty()) {
            return null;
        }
        try {
            CronExpression expression = new CronExpression(cron);
            return expression.getNextValidTimeAfter(new Date());
        } catch (ParseException e) {
            logger.error("parse cron error. " + cron, e);
            return null;
        }
    }

    /**
     * 构造上下文环境
     *
     * @param task    任务
     * @param configs 配置
     * @return 上下文
     * @throws Exception
     */
    protected TaskContext getTaskContext(final Task task, final List<Config> configs) throws Exception {
        // 从配置文件读取的数据
        TaskContext context = new TaskContext(parameters);
        // 从数据库读取的配置数据
        if (configs != null) {
            for (Config config : configs) {
                context.put(config.getKey(), config.getValue());
            }
        }
        // 从URL读取配置数据
        String url = task.getUrl();
        if (url != null && !url.isEmpty()) {
            if (!url.contains("://")) {
                url = task.getType() + "://" + url;
            }
            Map<String, String> urlParameters = URL.valueOf(url).getParameters();
            if (urlParameters != null) {
                context.put(urlParameters);
            }
        }
        return context;
    }

    /**
     * 获取异常堆栈信息
     *
     * @param e 异常
     * @return 异常字符串
     */
    protected String getException(final Throwable e) {
        if (e == null) {
            return null;
        }
        if (e instanceof TaskException) {
            if (e.getCause() != null) {
                return Exceptions.getError(e.getCause(), true, 2000);
            } else {
                return Exceptions.getError(e, false, 2000);
            }
        }
        return Exceptions.getError(e, true, 2000);
    }

    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public String getConfigGroup() {
        return configGroup;
    }

    public void setConfigGroup(String configGroup) {
        this.configGroup = configGroup;
    }

    public TaskDao getTaskDao() {
        return taskDao;
    }

    public void setTaskDao(TaskDao taskDao) {
        this.taskDao = taskDao;
    }

    public ConfigDao getConfigDao() {
        return configDao;
    }

    public void setConfigDao(ConfigDao configDao) {
        this.configDao = configDao;
    }
}
