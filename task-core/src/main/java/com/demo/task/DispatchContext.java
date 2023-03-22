package com.demo.task;

import com.jd.joyqueue.model.domain.Task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 任务分配上下文
 */
public class DispatchContext {
    // 存活的的执行器
    private Set<String> lives;
    // 新任务
    private List<Task> created;
    // 失败重试的任务
    private List<Task> retrys;
    // 所有已分配任务
    private List<Task> dispatched;
    // 最大任务数
    private int concurrency;
    // 当前分配执行的任务
    private List<NodeTask> nodes = new ArrayList<NodeTask>();

    public Set<String> getLives() {
        return lives;
    }

    public void setLives(Set<String> lives) {
        this.lives = lives;
    }

    public List<Task> getCreated() {
        return created;
    }

    public void setCreated(List<Task> created) {
        this.created = created;
    }

    public List<Task> getRetrys() {
        return retrys;
    }

    public void setRetrys(List<Task> retrys) {
        this.retrys = retrys;
    }

    public List<Task> getDispatched() {
        return dispatched;
    }

    public void setDispatched(List<Task> dispatched) {
        this.dispatched = dispatched;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public List<NodeTask> getNodes() {
        return nodes;
    }

    @Override
    public int hashCode() {
        int result = lives == null ? 0 : lives.hashCode();
        result = 31 * result + hashCode(created);
        result = 31 * result + hashCode(retrys);
        result = 31 * result + hashCode(dispatched);
        result = 31 * result + concurrency;
        return result;
    }

    /**
     * 计算列表的散列值
     *
     * @param tasks 任务列表
     * @return 散列值
     */
    protected int hashCode(final List<Task> tasks) {
        int hashCode = 1;
        Iterator<Task> i = tasks.iterator();
        while (i.hasNext()) {
            hashCode = 31 * hashCode + hashCode(i.next());
        }
        return hashCode;
    }

    /**
     * 计算任务的散列值
     *
     * @param task 任务
     * @return 散列值
     */
    protected int hashCode(final Task task) {
        if (task == null) {
            return 0;
        }
        int result = (int) (task.getId() ^ (task.getId() >>> 32));
        // 增加修改时间，避免只有重试任务，一致不成功，后续就调度不了了
        result = 31 * result + (task.getUpdateTime() != null ? task.getUpdateTime().hashCode() : 0);
        return result;
    }

    /**
     * 序列化为字符串
     *
     * @param nodes 节点任务
     * @return 字符串
     */
    public static String marshal(final List<NodeTask> nodes) {
        StringBuilder builder = new StringBuilder();
        if (nodes != null) {
            int count = 0;
            List<Task> tasks;
            for (NodeTask node : nodes) {
                if (node.size() > 0) {
                    if (count > 0) {
                        builder.append(';');
                    }
                    builder.append(node.name).append(':');
                    for (int i = 0; i < node.tasks.size(); i++) {
                        if (i > 0) {
                            builder.append(',');
                        }
                        builder.append(node.tasks.get(i).getId());
                    }
                    count++;
                }
            }
        }
        return builder.toString();
    }

    /**
     * 反序列化
     *
     * @param value 字符串
     * @return 节点任务
     */
    public static List<NodeTask> unmarshal(final String value) {
        List<NodeTask> result = new ArrayList<NodeTask>();
        if (value == null || value.isEmpty()) {
            return result;
        }
        String[] ids;
        String[] values = value.split(";");
        List<Task> tasks;
        String name;
        for (String one : values) {
            tasks = new ArrayList<Task>();
            int pos = one.lastIndexOf(':');
            if (pos > 0) {
                name = one.substring(0, pos);
                one = one.substring(pos + 1);
            } else {
                name = null;
                one = null;
            }
            if (name != null) {
                tasks = new ArrayList<Task>();
                if (one != null && !one.isEmpty()) {
                    ids = one.split(",");
                    for (String id : ids) {
                        tasks.add(new Task(Long.valueOf(id)));
                    }
                }
                result.add(new NodeTask(name, tasks));
            }
        }

        return result;
    }

    /**
     * 节点任务
     */
    public static class NodeTask {
        // 名称
        public String name;
        // 任务
        public List<Task> tasks;
        // 平均任务数
        public int average;
        // 并发任务数
        public int concurrency;
        // 被移除的任务，优先级升序排序
        public List<Task> excesses;

        public NodeTask(String name, List<Task> tasks) {
            this.name = name;
            this.tasks = tasks;
        }

        public NodeTask(String name, List<Task> tasks, int concurrency) {
            this.name = name;
            this.tasks = tasks;
            this.concurrency = concurrency;
        }

        public int size() {
            return tasks == null ? 0 : tasks.size();
        }

        public void addExcess(Task task) {
            if (excesses == null) {
                excesses = new ArrayList<Task>();
            }
            excesses.add(task);
        }
    }

}
