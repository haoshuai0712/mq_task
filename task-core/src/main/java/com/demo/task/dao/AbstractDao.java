package com.demo.task.dao;

import javax.sql.DataSource;

public class AbstractDao {

    protected DataSource dataSource;

    protected AbstractDao() {
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}