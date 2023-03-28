package com.demo.service.test.impl;

import com.demo.model.TestModel;
import com.demo.repository.TestRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestServiceImplTest {

    @Mock
    private TestRepository mockTestRepository;

    @InjectMocks
    private TestServiceImpl testServiceImplUnderTest;

    @Test
    public void testGet() {
        // Setup
        // Configure TestRepository.get(...).
        final TestModel testModel = new TestModel();
        testModel.setStr1("str1");
        testModel.setNum1(0);
        when(mockTestRepository.get("str1")).thenReturn(testModel);

        // Run the test
        final TestModel result = testServiceImplUnderTest.get("str1");

        // Verify the results
    }

    @Test
    public void testInsert() {
        // Setup
        final TestModel testModel = new TestModel();
        testModel.setStr1("str1");
        testModel.setNum1(0);

        when(mockTestRepository.insert(any(TestModel.class))).thenReturn(0);

        // Run the test
        final int result = testServiceImplUnderTest.insert(testModel);

        // Verify the results
        assertEquals(0, result);
    }
}
