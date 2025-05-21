package com.microsoft.durabletask;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;

public class TestRetryExtension implements TestExecutionExceptionHandler, BeforeEachCallback, AfterEachCallback {
    private static final int MAX_RETRIES = 3;
    private int currentRetries = 0;

    @Override
    public void beforeEach(ExtensionContext context) {
        currentRetries = 0;
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        if (currentRetries < MAX_RETRIES - 1) {
            currentRetries++;
            System.err.println(String.format("Test '%s' failed on attempt %d/%d", context.getDisplayName(), currentRetries + 1, MAX_RETRIES));
            context.getRequiredTestMethod().invoke(context.getRequiredTestInstance());
        } else {
            System.err.println(String.format("Test '%s' failed after %d attempts", context.getDisplayName(), MAX_RETRIES));
            throw throwable;
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        currentRetries = 0;
    }
}
