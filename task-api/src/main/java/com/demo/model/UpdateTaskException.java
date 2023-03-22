package com.demo.model;

import java.sql.SQLException;

/**
 * 更新任务状态异常
 */
public class UpdateTaskException extends SQLException {

    public UpdateTaskException(SQLException e) {
        super(e.getMessage(), e.getSQLState(), e.getErrorCode(), e.getCause());
    }

}
