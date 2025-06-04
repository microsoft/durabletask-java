package com.microsoft.durabletask;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import java.lang.reflect.Method;

public class TestRetryExtension implements InvocationInterceptor {
    private static final int MAX_RETRIES = 3;

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, 
                                   ReflectiveInvocationContext<Method> invocationContext, 
                                   ExtensionContext extensionContext) throws Throwable {
        
        Throwable lastException = null;
        
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                invocation.proceed();
                return; // Success, exit the retry loop
            } catch (Throwable throwable) {
                lastException = throwable;
                if (attempt < MAX_RETRIES) {
                    System.err.println(String.format("Test '%s' failed on attempt %d/%d: %s", 
                        extensionContext.getDisplayName(), attempt, MAX_RETRIES, throwable.getMessage()));
                } else {
                    System.err.println(String.format("Test '%s' failed after %d attempts", 
                        extensionContext.getDisplayName(), MAX_RETRIES));
                }
            }
        }
        
        // If we get here, all retries failed
        throw lastException;
    }
}
