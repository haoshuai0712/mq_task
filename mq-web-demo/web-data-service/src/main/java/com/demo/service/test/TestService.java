package com.demo.service.test;

import com.demo.model.TestModel;

public interface TestService {
    TestModel get(String str1);

    int insert(TestModel testModel);
}
