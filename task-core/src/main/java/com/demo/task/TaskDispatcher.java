package com.demo.task;

import com.google.common.base.Strings;
import com.jd.joyqueue.model.RepositoryException;
import com.jd.joyqueue.model.domain.Task;
import com.jd.joyqueue.registry.Registry;
import com.jd.joyqueue.registry.listener.ChildrenEvent;
import com.jd.joyqueue.registry.listener.ChildrenListener;
import com.jd.joyqueue.registry.listener.LeaderEvent;
import com.jd.joyqueue.registry.listener.LeaderListener;
import com.jd.joyqueue.registry.util.Path;
import com.jd.joyqueue.task.algo.DispatchAlgorithm;
import com.jd.joyqueue.task.dao.TaskDao;
import com.jd.laf.extension.ExtensionManager;
import org.joyqueue.toolkit.concurrent.EventBus;
import org.joyqueue.toolkit.concurrent.EventListener;
import org.joyqueue.toolkit.service.Service;
import org.joyqueue.toolkit.time.DateTime;
import org.joyqueue.toolkit.validate.annotation.NotEmpty;
import org.joyqueue.toolkit.validate.annotation.NotNull;
import org.joyqueue.toolkit.validate.annotation.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskDispatcher extends Service {
    private static final Logger logger = LoggerFactory.getLogger(TaskDispatcher.class);
    // 注册中心
    @NotNull
    private Registry registry;
    @NotEmpty
    private String application;
    @NotNull
    private TaskDao taskDao;
    // 存活节点
    private String livePath;
    // 任务计划节点
    private String schedulePath;
    // 选举节点
    private String leaderPath;
    @Range(min = 1)
    private long interval = 1000L;
    // 空闲事件间隔
    @Range(min = 1)
    private long idleTime = 1000L;
    // 每个实例并发任务数量
    @Range(min = 1)
    private int concurrency = 20;
    // 数据散列
    private int lastHash = 0;
    // 是否是Leader
    protected AtomicBoolean leader = new AtomicBoolean(false);
    // 存活节点名称
    private Set<String> lives = new CopyOnWriteArraySet<String>();
    // 存活节点监听器
    private ChildrenListener liveListener = new LiveListener();
    // Leader节点标示
    private EventBus<DispatchType> tasks = new TaskEventBus(new DispatchListener());
    // Leader监听器
    private LeaderListener leaderListener = new MyLeaderListener();

    @Override
    protected void validate() throws Exception {
        super.validate();

        livePath = Path.concat(application, "/task/live");
        schedulePath = Path.concat(application, "/task/executor");
        leaderPath = Path.concat(application, "/task/leader");
        Path.validate(livePath);
        Path.validate(schedulePath);
        Path.validate(leaderPath);
    }

    @Override
    protected void doStart() throws Exception {
        logger.info("TaskDispatcher doStart.");
        lastHash = 0;
        registry.addListener(leaderPath, leaderListener);
        super.doStart();
    }

    @Override
    protected void doStop() {
        if (registry != null) {
            registry.removeListener(livePath, liveListener);
            registry.removeListener(leaderPath, leaderListener);
        }
        lives.clear();
        tasks.stop();
        super.doStop();
    }

    /**
     * 根据任务分发类型分配任务
     *
     * @param dispatchType 任务类型
     * @throws Exception
     */
    protected void dispatch(final DispatchType dispatchType) throws Exception {
        DispatchAlgorithm algorithm = getDispatchAlgorithm(dispatchType.name());
        if (algorithm == null) {
            throw new IllegalStateException("DispatchAlgorithm is not implement," + dispatchType.name());
        }

        // 根据状态查询，目前要执行的任务
        final Date now = new Date();
        logger.info(String.format("start dispatching task."));
        List<Task> executings = new ArrayList<Task>(2000);
        executings.addAll(taskDao.findByStatus(Task.DISPATCHED));
        executings.addAll(taskDao.findByStatus(Task.RUNNING));
        List<Task> retrys = taskDao.findByStatus(Task.FAILED_RETRY);
        List<Task> created = taskDao.findByStatus(Task.NEW);

        DispatchContext context = new DispatchContext();
        context.setLives(lives);
        context.setConcurrency(concurrency);
        context.setDispatched(executings);
        context.setRetrys(retrys);
        context.setCreated(created);
        logger.info("load tasks {} from database.", executings.size() + retrys.size() + created.size());

        // 去掉依赖未完成或互斥的任务
        remove(context);

        // 对任务的hashCode，增加了修改时间，避免只有重试任务，一致不成功，任务就不会调度了
        int currentHash = context.hashCode();
        if (lastHash == currentHash) {
            logger.info(String.format("update task, task is not changed."));
            return;
        }

        algorithm.dispatch(context);
        dispatchTask(context.getNodes());
        resetTask(context.getNodes());
        updateZK(context.getNodes());
        //操作成功更新hashcode
        lastHash = currentHash;
    }

    /**
     * 更新zookeeper
     *
     * @param nodes 节点任务
     * @throws Exception
     */
    protected void updateZK(final List<DispatchContext.NodeTask> nodes) throws Exception {
        String value = DispatchContext.marshal(nodes);
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        registry.update(schedulePath, data);
        logger.info(String.format("update task %s", value));
    }

    /**
     * 更新数据库状态
     *
     * @param nodes 节点任务
     * @return 已经派送的任务
     */
    protected void dispatchTask(final List<DispatchContext.NodeTask> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        // 遍历节点
        for (DispatchContext.NodeTask node : nodes) {
            // 待调度的任务列表，降序排序
            List<Task> tasks = node.tasks;
            // 多余的任务列表，升序排序
            List<Task> excesses = node.excesses;
            // 补充的数量
            int count = 0;
            // 遍历任务
            for (int i = tasks.size() - 1; i >= 0; i--) {
                // 更新数据库状态
                if (!dispatchTask(node, tasks.get(i))) {
                    tasks.remove(i);
                    count++;
                }
            }
            // 有任务状态发生了变更，尝试从删除的列表里面补充一下
            if (count > 0 && excesses != null) {
                // 删除列表是升序的，倒序遍历
                for (int i = excesses.size() - 1; i >= 0; i--) {
                    if (count <= 0) {
                        break;
                    }
                    if (dispatchTask(node, excesses.get(i))) {
                        tasks.add(excesses.remove(i));
                        count--;
                    }
                }
            }
        }
    }

    /**
     * 派发任务
     *
     * @param node 节点
     * @param task 任务
     * @return 成功标识
     */
    protected boolean dispatchTask(final DispatchContext.NodeTask node, final Task task) {
        // 变更了节点、重试和新增任务需要更新数据
        if (!node.name.equals(task.getOwner())
                || task.getStatus() == Task.FAILED_RETRY || task.getStatus() == Task.NEW) {
            try {
                task.setOwner(node.name);
                task.setStatus(Task.DISPATCHED);
                if (taskDao.update(task) == 0) {
                    return false;
                }
                return true;
            } catch (Exception e) {
                logger.warn("dispatch task " + task.getId() + " failure!", e);
                return false;
            }
        }
        return true;

    }

    /**
     * 重置状态
     *
     * @param nodes 任务节点
     */
    protected void resetTask(final List<DispatchContext.NodeTask> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        for (DispatchContext.NodeTask node : nodes) {
            if (node.excesses != null) {
                for (Task task : node.excesses) {
                    try {
                        task.setStatus(Task.NEW);
                        taskDao.update(task);
                    } catch (Exception e) {
                        logger.error("retry task " + task.getId() + " failure!", e);
                    }
                }
            }
        }
    }

    /**
     * 去掉依赖为完成或互斥的任务
     *
     * @param context 上下文
     */
    protected void remove(final DispatchContext context) {
        removeDuplicated(context);
        removeMutex(context);
    }

    /**
     * 删除多次查询数据造成的数据重复问题
     *
     * @param context 上下文
     */
    protected void removeDuplicated(final DispatchContext context) {
        int size = context.getDispatched().size() + context.getRetrys().size() + context.getCreated().size();
        Set<Long> uniques = new HashSet<Long>(size);
        // 按照执行、重试和新建的顺序加载的列表，越往后数据约新
        removeDuplicated(uniques, context.getCreated());
        removeDuplicated(uniques, context.getRetrys());
        removeDuplicated(uniques, context.getDispatched());
    }

    /**
     * 同一个作业，去掉前面步骤未执行完成的任务
     *
     * @param uniques 唯一
     * @param tasks   任务列表
     */
    protected void removeDuplicated(final Set<Long> uniques, List<Task> tasks) {
        Task task;
        // 降序，因为按照执行、重试和新建的顺序加载的列表，越往后数据约新
        for (int i = tasks.size() - 1; i >= 0; i--) {
            task = tasks.get(i);
            if (!uniques.add(task.getId())) {
                tasks.remove(i);
            }
        }

    }

    /**
     * 执行器存活监听
     */
    protected class LiveListener implements ChildrenListener {
        @Override
        public void onEvent(ChildrenEvent event) {
            logger.info("TaskDispatcher LiveListener onEvent Running.");
            // 当新添加执行器、执行器宕机时通知 任务分配
            if (!isStarted() || !leader.get()) {
                return;
            }
            String node = Path.node(event.getPath());
            if (event.getType() == ChildrenEvent.ChildrenEventType.CHILD_REMOVED) {
                lives.remove(node);
            } else if (event.getType() == ChildrenEvent.ChildrenEventType.CHILD_CREATED) {
                lives.add(node);
            }
            tasks.add(DispatchType.LOAD_BALANCE);
        }
    }

    /**
     * 选举监听器，获取Leader后注册存活监听器，进行任务派发
     */
    protected class MyLeaderListener implements LeaderListener {
        @Override
        public void onEvent(final LeaderEvent event) {
            logger.info("TaskDispatcher MyLeaderListener onEvent Running.");
            // 获取Leader
            if (event.getType() == LeaderEvent.LeaderEventType.TAKE) {
                // 空闲的时候触发onIdle方法
                tasks.setIdleTime(idleTime);
                // 事件进行合并
                tasks.setInterval(interval);
                try {
                    tasks.start();
                } catch (Exception ignored) {
                    // 事件总线不会有异常
                }
                leader.set(true);
                registry.addListener(livePath, liveListener);
            } else {
                leader.set(false);
                registry.removeListener(livePath, liveListener);
                tasks.stop();
            }
        }
    }

    /**
     * 任务派发事件监听器
     */
    protected class DispatchListener implements EventListener<DispatchType> {

        @Override
        public void onEvent(final DispatchType event) {
            readLock.lock();
            try {
                if (!isStarted() || !leader.get()) {
                    return;
                }
                logger.info("TaskDispatcher DispatchListener onEvent, dispatching...");
                dispatch(event);
            } catch (RepositoryException e) {
                // 如果数据库出现异常，则清理缓存的任务散列值
                lastHash = 0;
                logger.error(e.getMessage(), e);
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            } finally {
                readLock.unlock();
            }
        }

    }

    /**
     * 调度事件管理器
     */
    protected class TaskEventBus extends EventBus<DispatchType> {

        public TaskEventBus(EventListener<DispatchType> listener) {
            super("TaskDispatcher", listener);
        }

        @Override
        protected void publish(List<Ownership> events) {
            // 合并事件，选择最优的负载算法
            if (events == null || events.isEmpty()) {
                return;
            }
            Ownership max = null;
            for (Ownership event : events) {
                if (max == null || max.event == DispatchType.LOAD_BALANCE) {
                    max = event;
                }
            }
            publish(max);
        }

        @Override
        protected void onIdle() {
            if (isStarted() && leader.get()) {
                // 空闲检测
                tasks.add(DispatchType.MINIMUM);
            }
        }
    }

    private com.jd.joyqueue.task.algo.DispatchAlgorithm getDispatchAlgorithm(String name) {

        if (Strings.isNullOrEmpty(name)) {
            logger.warn("The DispatchAlgorithm's type can not be null or empty!");
            return null;
        }

        Iterable<com.jd.joyqueue.task.algo.DispatchAlgorithm> collectors = ExtensionManager.getOrLoadExtensions(com.jd.joyqueue.task.algo.DispatchAlgorithm.class);
        Iterator<DispatchAlgorithm> it = collectors.iterator();
        while (it.hasNext()) {
            com.jd.joyqueue.task.algo.DispatchAlgorithm executor = it.next();

            if (name.equalsIgnoreCase(executor.getType().toString())) {
                return executor;
            }
        }
        logger.warn("Do not find the correct DispatchAlgorithm for DispatchAlgorithm type {}", name);
        return null;
    }

    /**
     * 异常掉互斥的任务，支持*号结尾前缀
     *
     * @param context 上下文
     */
    protected void removeMutex(final DispatchContext context) {
        List<Task> tasks = new ArrayList<Task>();
        tasks.addAll(context.getDispatched());
        tasks.addAll(context.getRetrys());
        tasks.addAll(context.getCreated());
        // 按序号排序
        Collections.sort(tasks, new TaskComparator());
        // 前缀匹配
        List<String> prefixes = new ArrayList<String>();
        // 查找互斥的任务
        Set<String> mutexes = new HashSet<String>(tasks.size());
        List<Task> excludes = new ArrayList<Task>();
        String mutex;
        int length;
        // 是否要过滤掉
        boolean exclude;
        // 是否是前缀匹配模式
        boolean prefix;
        // 遍历所有任务
        for (Task task : tasks) {
            // 互斥量
            mutex = task.getMutex() == null ? null : task.getMutex().trim();
            exclude = false;
            prefix = false;
            // 是否是前缀匹配
            length = mutex == null ? 0 : mutex.length();
            if (length > 0 && mutex.charAt(length - 1) == '*') {
                prefix = true;
                if (length == 1) {
                    // 匹配所有
                    if (!prefixes.isEmpty() || !mutexes.isEmpty()) {
                        // 已经有其它任务了
                        exclude = true;
                    } else {
                        prefixes.add("");
                    }
                } else {
                    // 前缀
                    mutex = mutex.substring(0, length - 1);
                    for (String v : mutexes) {
                        if (!v.isEmpty() && v.startsWith(mutex)) {
                            // 已经在运行
                            exclude = true;
                            break;
                        }
                    }
                    if (!exclude) {
                        // 判断该前缀是否已经在运行
                        for (String v : prefixes) {
                            // 单个*号匹配，存放的空字符串
                            if (v.isEmpty() || v.startsWith(mutex) || mutex.startsWith(v)) {
                                // 已经在运行
                                exclude = true;
                                break;
                            }
                        }
                    }
                }
            } else {
                // 非前缀匹配
                if (!prefixes.isEmpty()) {
                    for (String v : prefixes) {
                        if (v.isEmpty() || (length > 0 && mutex.startsWith(v))) {
                            exclude = true;
                            break;
                        }
                    }
                }
                if (!exclude && length > 0 && mutexes.contains(mutex)) {
                    exclude = true;
                }
            }
            if (exclude) {
                excludes.add(task);
                logger.info(String.format("remove mutex task, %s", task.toString()));
            } else if (prefix) {
                prefixes.add(mutex);
            } else {
                mutexes.add(mutex == null ? "" : mutex);
            }
        }
        context.getDispatched().removeAll(excludes);
        context.getRetrys().removeAll(excludes);
        context.getCreated().removeAll(excludes);
    }

    /**
     * 任务排序
     */
    protected static class TaskComparator implements Comparator<Task> {
        @Override
        public int compare(final Task o1, final Task o2) {
            Date d1 = getRetryTime(o1);
            Date d2 = getRetryTime(o2);
            int result = d1.compareTo(d2);
            if (result == 0) {
                long value = o1.getId() - o2.getId();
                result = value == 0 ? 0 : (value > 0 ? 1 : -1);
            }
            return result;
        }

        /**
         * 获取冲死时间
         *
         * @param task 任务
         * @return
         */
        protected Date getRetryTime(final Task task) {
            if (task.getRetryTime() != null) {
                return task.getRetryTime();
            } else if (task.getUpdateTime() != null) {
                return task.getUpdateTime();
            } else if (task.getCreateTime() != null) {
                return task.getCreateTime();
            }
            return DateTime.of(0).date();
        }
    }


    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public TaskDao getTaskDao() {
        return taskDao;
    }

    public void setTaskDao(TaskDao taskDao) {
        this.taskDao = taskDao;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }
}
