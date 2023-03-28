package com.demo.service.test.impl;

import com.demo.model.TestModel;
import com.demo.repository.TestRepository;
import com.demo.service.test.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TestServiceImpl implements TestService {

    @Autowired
    private TestRepository testRepository;

    @Override
    public TestModel get(String str1) {
        return testRepository.get(str1);
    }

    @Override
    public int insert(TestModel testModel) {
        return testRepository.insert(testModel);
    }
}
