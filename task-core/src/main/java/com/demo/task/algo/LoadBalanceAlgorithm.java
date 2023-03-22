package com.demo.task.algo;

import com.jd.joyqueue.model.domain.Task;
import com.demo.task.DispatchContext;
import com.demo.task.DispatchType;

import java.util.Collections;
import java.util.List;

/**
 * 负载均衡算法
 */
public class LoadBalanceAlgorithm extends MinimumAlgorithm {

    @Override
    protected void doLoadBalance(List<DispatchContext.NodeTask> executorTasks) {
        if (executorTasks == null || executorTasks.isEmpty()) {
            return;
        }
        int taskCount = 0;
        for (DispatchContext.NodeTask executorTask : executorTasks) {
            taskCount += executorTask.size();
        }
        int average = (int) Math.ceil(taskCount * 1.0 / executorTasks.size());

        // 按任务数升序排序
        Collections.sort(executorTasks, (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }
            return o1.size() - o2.size();
        });
        //从任务数最多的节点开始
        for (int i = executorTasks.size() - 1; i >= 0; i--) {
            DispatchContext.NodeTask executorTask = executorTasks.get(i);
            List<Task> tasks = executorTask.tasks;
            int size = tasks.size();
            int count = size - average;
            if (count <= 0) {
                break;
            }
            // 按状态和优先级降序排序
            Collections.sort(tasks, new TaskComparator(false));
            // 倒序遍历，把优先级最低的任务分配到当前任务最少的节点
            //for (int j = size - 1; j >= (size - count); j--) {
            for (int j = size - 1; j >= 0; j--) {
                //因为有些任务是指定分配到某个节点的，所以这里还是需要遍历全部任务，才能尽量做到节点间任务平衡。
                if ((tasks.size() - average) <= 0) {
                    break;
                }
                // 获取最小任务数的节点
                DispatchContext.NodeTask target = null;
                // 顺序遍历
                for (int k = 0; k < i; k++) {
                    DispatchContext.NodeTask et = executorTasks.get(k);
                    if (et.size() >= average) {
                        // 超过了平均数 继续找下一个 直到k=i
                        continue;
                    }
                    // 获取最小任务数
                    if (target == null) {
                        target = et;
                    } else if (target.size() > et.size()) {
                        target = et;
                    }
                }
                //这个任务如果是指定到该节点，就不能删除，如果是属于可以任务分配执行节点的任务，就可以移到别的节点执行,
                //如果指定分配到特定节点的任务过多,可能造成节点间任务分配不均，不过这也没办法，add by liningbo.
                if (target != null && tasks.get(j).getDispatchType() == 0) {
                    // 待删除的任务
                    Task task = tasks.remove(j);
                    // 添加到最小任务数中
                    target.addExcess(task);
                }
            }
        }
    }

    @Override
    public String getType() {
        return DispatchType.LOAD_BALANCE.name();
    }

}

