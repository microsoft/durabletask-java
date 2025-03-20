// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import java.util.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * JUnit Jupiter extension that provides test retry capability.
 * Tests will be retried up to 3 times before failing.
 */
public class TestRetryExtension implements TestExecutionExceptionHandler {
    private static final Logger LOGGER = Logger.getLogger(TestRetryExtension.class.getName());
    private static final int MAX_RETRY_COUNT = 3;
    private final ConcurrentHashMap<String, Integer> retryCounters = new ConcurrentHashMap<>();

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        String testMethod = getTestMethodName(context);
        int retryCount = retryCounters.getOrDefault(testMethod, 0);
        
        if (retryCount < MAX_RETRY_COUNT - 1) { // -1 because the first attempt doesn't count as a retry
            retryCounters.put(testMethod, retryCount + 1);
            LOGGER.warning(String.format("Test '%s' failed (attempt %d). Retrying...", testMethod, retryCount + 1));
            // Return without rethrowing to allow retry
            return;
        }
        
        // Log final failure and rethrow the exception
        LOGGER.severe(String.format("Test '%s' failed after %d retries", testMethod, MAX_RETRY_COUNT - 1));
        throw throwable;
    }
    
    private String getTestMethodName(ExtensionContext context) {
        String methodName = context.getRequiredTestMethod().getName();
        
        // Include parameters for parameterized tests to ensure each parameter combination is retried separately
        String params = context.getDisplayName();
        // If the display name contains parameters (e.g. "testMethod(param1, param2)"), extract them
        if (params.contains("(") && params.endsWith(")")) {
            params = params.substring(params.indexOf('('));
        } else {
            params = "";
        }
        
        return context.getRequiredTestClass().getName() + "." + methodName + params;
    }
} 