package com.demo.task.dao;

import org.joyqueue.domain.Config;

import java.util.ArrayList;
import java.util.List;

public class ConfigDao extends AbstractDao {

    public List<Config> findByGroup(String configGroup) {
        if (configGroup == null || configGroup.isEmpty()) {
            return new ArrayList();
        }
        return null;
    }
}