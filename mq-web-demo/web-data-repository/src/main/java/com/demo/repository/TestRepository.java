package com.demo.repository;

import com.demo.model.TestModel;
import org.springframework.stereotype.Repository;

@Repository
public interface TestRepository {

    int insert(TestModel testModel);

    TestModel get(String str1);
}
