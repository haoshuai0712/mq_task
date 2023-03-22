package com.demo.model;

public class TaskException extends Exception {

    private static final long serialVersionUID = 8582203550804100822L;
    public static final int RETRY_INTERVAL = 10 * 1000;
    // 是否需要重试，false代表不重试，true代表需要重试
    private Action action = Action.STOP;
    // 延迟毫秒数(默认10秒)
    private int delay = RETRY_INTERVAL;

    public TaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskException(String message) {
        super(message);
    }

    public TaskException(Throwable cause) {
        super(cause);
    }

    public TaskException(String message, Throwable cause, Action action) {
        this(message, cause, action, 0);
    }

    public TaskException(String message, Throwable cause, Action action, int delay) {
        super(message, cause);
        this.action = action;
        this.delay = delay;
    }

    public TaskException(String message, Action action) {
        this(message, action, 0);
    }

    public TaskException(String message, Action action, int delay) {
        super(message);
        this.action = action;
        this.delay = delay;
    }

    public TaskException(Throwable cause, Action action) {
        this(cause, action, 0);
    }

    public TaskException(Throwable cause, Action action, int delay) {
        super(cause);
        this.action = action;
        this.delay = delay;
    }

    public Action getAction() {
        if (action == null) {
            return Action.STOP;
        }
        return action;
    }

    public int getDelay() {
        return delay;
    }

    public enum Action {
        /**
         * 停止
         */
        STOP,
        /**
         * 立即结束退出
         */
        STOP_NOW,
        /**
         * 重试
         */
        RETRY,
        /**
         * 重试不增加计数器
         */
        RETRY_IMMUTABLE
    }
}
