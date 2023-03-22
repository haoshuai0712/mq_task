package com.demo.task.algo;

import com.demo.task.DispatchContext;

/**
 * 任务分配算法
 */
public interface DispatchAlgorithm {

    /**
     * 任务分配
     *
     * @param context
     * @return
     */
    void dispatch(DispatchContext context);

    String getType();

}

