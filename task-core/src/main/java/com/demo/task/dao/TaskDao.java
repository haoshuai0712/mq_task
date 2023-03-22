package com.demo.task.dao;

import com.demo.model.UpdateTaskException;
import com.jd.joyqueue.model.domain.Task;
import org.joyqueue.model.domain.Identity;
import org.joyqueue.toolkit.db.DaoUtil;
import org.joyqueue.toolkit.time.CronExpression;
import org.joyqueue.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

public class TaskDao extends AbstractDao {

    private static Logger logger = LoggerFactory.getLogger(TaskDao.class);

    public static final String INSERT_SQL =
            "INSERT INTO task(TYPE,refer_id,priority,daemons,owner,url,cron,dispatch_type,retry,retry_count," +
                    "retry_time,create_time,create_by,update_time,update_by,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?," +
                    "?,?,?)";

    public static final String FIND_BY_ID_SQL = "SELECT id,type,refer_id,priority,daemons,owner,url,cron," +
            "dispatch_type,retry,retry_count,retry_time," +
            "" + "create_time,create_by,update_time,update_by,status,exception FROM task where id=?";

    public static final String FIND_BY_STATUS_SQL = "SELECT id,type,refer_id,priority,daemons,owner,url,cron," +
            "dispatch_type,retry,retry_count,retry_time," +
            "" + "create_time,create_by,update_time,update_by,status,exception FROM task where status=? and (retry_time is null or retry_time < now())";

    public static final String UPDATE_SQL =
            "UPDATE task SET owner=?,priority=?,retry=?,retry_count=?,retry_time=?,EXCEPTION=?,update_time=?," +
                    "update_by=?,status=? WHERE id=?";

    public static final String INSERT_HISTORY_SQL =
            "insert into task(type,refer_id,priority,daemons,owner,url,cron,dispatch_type,retry,retry_count," +
                    "retry_time,create_by,create_time,update_by,update_time,status) select type,refer_id,priority," +
                    "daemons,owner,url,cron,dispatch_type,retry,retry_count,retry_time,create_by,now()," +
                    "update_by,now(),? from task where id=?";

    public static final String UPDATE_RETRY_SQL =
            "UPDATE task SET create_time=now(), update_time=now(), update_by=?, status=?, retry_time=? WHERE id=?";

    public long add(final Task target) throws Exception {
        if (target == null) {
            return 0;
        }
        DaoUtil.insert(dataSource, target, INSERT_SQL, new DaoUtil.InsertCallback<Task>() {
            @Override
            public void before(final PreparedStatement statement, final Task target) throws Exception {
                target.setStatus(Task.NEW);
                target.setCreateTime(new Date(SystemClock.getInstance().now()));
                target.setUpdateTime(target.getCreateTime());

                if (target.getRetryTime() == null) {
                    if (target.getCron() != null && !target.getCron().isEmpty()) {
                        try {
                            CronExpression expression = new CronExpression(target.getCron());
                            target.setRetryTime(expression.getNextValidTimeAfter(target.getCreateTime()));
                            if (target.getRetryTime() == null) {
                                target.setRetryTime(target.getCreateTime());
                            }
                        } catch (ParseException e) {
                            throw new Exception(e.getMessage(), e);
                        }
                    }
                }
                statement.setString(1, target.getType());
                statement.setLong(2, target.getReferId());
                statement.setInt(3, target.getPriority());
                statement.setBoolean(4, target.isDaemons());
                statement.setString(5, target.getOwner());
                statement.setString(6, target.getUrl());
                statement.setString(7, target.getCron());
                statement.setInt(8, target.getDispatchType());
                statement.setBoolean(9, target.isRetry());
                statement.setInt(10, target.getRetryCount());
                statement.setTimestamp(11, new Timestamp(target.getRetryTime().getTime()));
                statement.setTimestamp(12, new Timestamp(target.getCreateTime().getTime()));
                statement.setLong(13, target.getCreateBy().getId());
                statement.setTimestamp(14, new Timestamp(target.getUpdateTime().getTime()));
                statement.setLong(15, target.getUpdateBy().getId());
                statement.setInt(16, target.getStatus());
            }

            @Override
            public void after(ResultSet rs, Task target) throws Exception {
                target.setId(rs.getLong(1));
            }
        });

        return target.getId();
    }

    public Task findById(long taskId) {
        Task task = null;
        try {
            task = DaoUtil.queryObject(dataSource, FIND_BY_ID_SQL, new DaoUtil.QueryCallback<Task>() {
                @Override
                public Task map(final ResultSet rs) throws Exception {
                    Task target = createTask(rs);
                    return target;
                }

                @Override
                public void before(final PreparedStatement statement) throws Exception {
                    statement.setLong(1, taskId);
                }
            });
        } catch (Exception e) {
            logger.error("findById error",e);
        }
        return task;
    }

    public int update(Task task) throws Exception {
        int result = 0;
        if (task == null) {
            return result;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Update task {}", task);
            logger.debug("Update task exception {}", task.getException());
        }

        try {
            result = DaoUtil.update(dataSource, task, UPDATE_SQL, (statement, target) -> {
                target.setUpdateTime(new Date(SystemClock.getInstance().now()));
                statement.setString(1, target.getOwner());
                statement.setInt(2, target.getPriority());
                statement.setBoolean(3, target.isRetry());
                statement.setInt(4, target.getRetryCount());
                if (target.getRetryTime()!= null) {
                    statement.setTimestamp(5, new Timestamp(target.getRetryTime().getTime()));
                }else {
                    statement.setTimestamp(5,null);
                }
                statement.setString(6, target.getException());
                statement.setTimestamp(7, new Timestamp(target.getUpdateTime().getTime()));
                statement.setLong(8, target.getUpdateBy().getId());
                statement.setInt(9, target.getStatus());
                statement.setLong(10, target.getId());
            });
        } catch (SQLException e) {
            throw new UpdateTaskException(e);
        }
        return result;

    }

    public void addHistoryAndRetry(Task target) throws SQLException {
        if (target == null) {
            return;
        }
        Connection connection = null;
        PreparedStatement statement = null;
        try {

            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            // 插入执行历史记录
            statement = connection.prepareStatement(INSERT_HISTORY_SQL);
            statement.setInt(1, Task.SUCCESSED);
            statement.setLong(2, target.getId());
            statement.executeUpdate();
            DaoUtil.close(null, statement, null);

            // 更新当前记录状态，设置下次执行时间
            statement = connection.prepareStatement(UPDATE_RETRY_SQL);
            statement.setLong(1, target.getUpdateBy().getId());
            statement.setInt(2, Task.NEW);
            statement.setTimestamp(3, new Timestamp(target.getRetryTime().getTime()));
            statement.setLong(4, target.getId());
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            logger.error("Exception occur while insert " + "and update task[{}]! ", target.getId(), e);
            connection.rollback();
            throw new UpdateTaskException(e);
        } finally {
            DaoUtil.close(connection, statement, null);
        }
    }

    public List<Task> findByStatus(int status) {
        List<Task> tasks = null;
        try {
            tasks = DaoUtil.queryList(dataSource, FIND_BY_STATUS_SQL, new DaoUtil.QueryCallback<Task>() {
                @Override
                public Task map(ResultSet rs) throws Exception {
                    return createTask(rs);
                }

                @Override
                public void before(PreparedStatement statement) throws Exception {
                    statement.setInt(1, status);
                }
            });
        } catch (Exception e) {
            logger.error("findByStatus error",e);
        }

        return tasks;
    }

    protected Task createTask(ResultSet rs) throws Exception {
        Task target = new Task();
        target.setId(rs.getLong(1));
        target.setType(rs.getString(2));
        target.setReferId(rs.getLong(3));
        target.setPriority(rs.getInt(4));
        target.setDaemons(rs.getBoolean(5));
        target.setOwner(rs.getString(6));
        target.setUrl(rs.getString(7));
        target.setCron(rs.getString(8));
        target.setDispatchType(rs.getInt(9));
        target.setRetry(rs.getBoolean(10));
        target.setRetryCount(rs.getInt(11));
        target.setRetryTime(rs.getTimestamp(12));
        target.setCreateTime(rs.getTimestamp(13));
        target.setCreateBy(new Identity(rs.getLong(14)));
        target.setUpdateTime(rs.getTimestamp(15));
        target.setUpdateBy(new Identity(rs.getLong(16)));
        target.setStatus(rs.getInt(17));
        target.setException(rs.getString(18));
        return target;
    }

}
