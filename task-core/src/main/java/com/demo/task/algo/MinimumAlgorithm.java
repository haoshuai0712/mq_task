package com.demo.task.algo;

import com.google.common.base.Strings;
import com.jd.joyqueue.model.domain.Task;
import com.demo.task.DispatchContext;
import com.demo.task.DispatchType;

import java.util.*;

import static com.jd.joyqueue.model.domain.Task.DispatchType.OWNER_FIRST;
import static com.jd.joyqueue.model.domain.Task.DispatchType.OWNER_MUST;

/**
 * 最少任务数算法
 *
 * @author hexiaofeng
 * @version 1.0.0
 * @since 13-1-16 上午11:26
 */
public class MinimumAlgorithm implements com.demo.task.algo.DispatchAlgorithm {
    @Override
    public void dispatch(final DispatchContext context) {

        // 获得新任务
        List<Task> created = context.getCreated();
        // 获取失败的daemon类型的任务
        List<Task> retrys = context.getRetrys();
        // 获取所有已分配任务
        List<Task> dispatched = context.getDispatched();
        // 存活节点
        Set<String> lives = context.getLives();

        // 宕机的执行器的任务
        List<Task> crashed = new ArrayList<Task>();

        // 存活机器的任务
        Map<String, List<Task>> nodeMap = new HashMap<String, List<Task>>();
        for (String live : lives) {
            nodeMap.put(live, new ArrayList<Task>());
        }

        List<Task> tasks;
        // 遍历任务,原来分配到哪些节点的任务，现在仍然分配到这些节点
        for (Task task : dispatched) {
            tasks = nodeMap.get(task.getOwner());
            if (tasks == null) {
                // 宕机任务
                crashed.add(task);
            } else {
                // 存活任务
                tasks.add(task);
            }
        }


        // 转换成节点任务
        List<DispatchContext.NodeTask> nodes = context.getNodes();
        for (Map.Entry<String, List<Task>> entry : nodeMap.entrySet()) {
            nodes.add(new DispatchContext.NodeTask(entry.getKey(), entry.getValue(), context.getConcurrency()));
        }

        // 分配新任务
        reassign(created, nodeMap);
        // 重新分配任务
        reassign(crashed, nodeMap);
        // 分配失败的任务
        reassign(retrys, nodeMap);
        // 做负载均衡
        doLoadBalance(nodes);
        // 删除多余的任务
        removeExcess(nodes);
    }

    @Override
    public String getType() {
        return DispatchType.MINIMUM.name();
    }

    /**
     * 新增任务、重试任务、宕机任务重新分配
     *
     * @param tasks
     * @param nodes
     */
    protected void reassign(final List<Task> tasks, final Map<String, List<Task>> nodes) {
        List<Task> node;

        // 遍历任务
        for (Task task : tasks) {
            // 指定分配到某个机器上的，找到该台机器并分配到其上
            if ((task.getDispatchType() == OWNER_MUST.value() || task.getDispatchType() == OWNER_FIRST.value())
                    && !Strings.isNullOrEmpty(task.getOwner())) {
                // 查找节点任务
                node = nodes.get(task.getOwner());
                if (node != null) {
                    // 找到节点，添加任务，继续
                    node.add(task);
                    continue;
                } else if (task.getDispatchType() == OWNER_MUST.value()) {
                    // 没有找到指定的机器(可能机器宕机了)，但是也不能分配到别的机器，这个任务暂时不分配，等机器恢复了，再分配
                    continue;
                }
            }

            // 没有指定分配到某个机器上，选择任务数最少的机器，分配这个任务
            node = null;
            for (List<Task> value : nodes.values()) {
                if (node == null || value.size() < node.size()) {
                    node = value;
                }
            }
            if (node != null) {
                node.add(task);
            }
        }
    }

    /**
     * 移除多余的任务
     *
     * @param nodes
     */
    protected void removeExcess(final List<DispatchContext.NodeTask> nodes) {
        // 遍历任务
        for (DispatchContext.NodeTask node : nodes) {
            // 判断任务数是否超过设置
            List<Task> tasks = node.tasks;
            int size = tasks.size();
            int deletes = size - node.concurrency;
            if (deletes <= 0) {
                continue;
            }
            // 按照状态和优先级降序排序
            Collections.sort(tasks, new TaskComparator(false));

            Task task;
            for (int i = size - 1; i >= 0; i--) {
                task = tasks.remove(i);
                node.addExcess(task);
                if (--deletes <= 0) {
                    break;
                }
            }
        }
    }

    /**
     * 负载均衡
     *
     * @param nodes 节点任务
     */
    protected void doLoadBalance(final List<DispatchContext.NodeTask> nodes) {
    }

    /**
     * 任务比较器
     */
    protected static class TaskComparator implements Comparator<Task> {
        private boolean ascend;

        public TaskComparator(boolean ascend) {
            this.ascend = ascend;
        }

        @Override
        public int compare(final Task o1, final Task o2) {
            if (o1 == o2) {
                return 0;
            }
            // 守护任务优先级最高
            if (o1.isDaemons() && !o2.isDaemons()) {
                return ascend ? 1 : -1;
            }else if (!o1.isDaemons() && o2.isDaemons()) {
                return ascend ? -1 : 1;
            }
            int result = ascend ? o1.getStatus() - o2.getStatus() : o2.getStatus() - o1.getStatus();
            if (result == 0) {
                result = ascend ? o1.getPriority() - o2.getPriority() : o2.getPriority() - o1.getPriority();
            }
            return result;
        }
    }
}
