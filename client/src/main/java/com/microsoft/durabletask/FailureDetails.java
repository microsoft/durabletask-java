// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TaskFailureDetails;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Class that represents the details of a task failure.
 * <p>
 * In most cases, failures are caused by unhandled exceptions in activity or orchestrator code, in which case instances
 * of this class will expose the details of the exception. However, it's also possible that other types of errors could
 * result in task failures, in which case there may not be any exception-specific information.
 */
public final class FailureDetails {
    private final String errorType;
    private final String errorMessage;
    private final String stackTrace;
    private final boolean isNonRetriable;

    FailureDetails(
            String errorType,
            @Nullable String errorMessage,
            @Nullable String errorDetails,
            boolean isNonRetriable) {
        this.errorType = errorType;
        this.stackTrace = errorDetails;

        // Error message can be null for things like NullPointerException but the gRPC contract doesn't allow null
        this.errorMessage = errorMessage != null ? errorMessage : "";
        this.isNonRetriable = isNonRetriable;
    }

    FailureDetails(Exception exception) {
        // Use the most specific exception in the chain for error type
        String errorType = exception.getClass().getName();
        String errorMessage = exception.getMessage();
        
        // Preserve null messages as empty string to match existing behavior
        if (errorMessage == null) {
            errorMessage = "";
        }
        
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.stackTrace = getFullStackTrace(exception);
        this.isNonRetriable = false;
    }

    FailureDetails(TaskFailureDetails proto) {
        this(proto.getErrorType(),
             proto.getErrorMessage(),
             proto.getStackTrace().getValue(),
             proto.getIsNonRetriable());
    }

    /**
     * Gets the exception class name if the failure was caused by an unhandled exception. Otherwise, gets a symbolic
     * name that describes the general type of error that was encountered.
     *
     * @return the error type as a {@code String} value
     */
    @Nonnull
    public String getErrorType() {
        return this.errorType;
    }

    /**
     * Gets a summary description of the error that caused this failure. If the failure was caused by an exception, the
     * exception message is returned.
     *
     * @return a summary description of the error
     */
    @Nonnull
    public String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Gets the stack trace of the exception that caused this failure, or {@code null} if the failure was caused by
     * a non-exception error.
     *
     * @return the stack trace of the failure exception or {@code null} if the failure was not caused by an exception
     */
    @Nullable
    public String getStackTrace() {
        return this.stackTrace;
    }

    /**
     * Returns {@code true} if the failure doesn't permit retries, otherwise {@code false}.
     * @return {@code true} if the failure doesn't permit retries, otherwise {@code false}.
     */
    public boolean isNonRetriable() {
        return this.isNonRetriable;
    }

    /**
     * Returns {@code true} if the task failure was provided by the specified exception type, otherwise {@code false}.
     * <p>
     * This method allows checking if a task failed due to a specific exception type by attempting to load the class
     * specified in {@link #getErrorType()}. If the exception class cannot be loaded for any reason, this method will
     * return {@code false}. Base types are supported by this method, as shown in the following example:
     * <pre>{@code
     * boolean isRuntimeException = failureDetails.isCausedBy(RuntimeException.class);
     * }</pre>
     *
     * @param exceptionClass the class representing the exception type to test
     * @return {@code true} if the task failure was provided by the specified exception type, otherwise {@code false}
     */
    public boolean isCausedBy(Class<? extends Exception> exceptionClass) {
        String actualClassName = this.getErrorType();
        try {
            // Try using reflection to load the failure's class type and see if it's a subtype of the specified
            // exception. For example, this should always succeed if exceptionClass is System.Exception.
            Class<?> actualExceptionClass = Class.forName(actualClassName);
            return exceptionClass.isAssignableFrom(actualExceptionClass);
        } catch (ClassNotFoundException ex) {
            // Can't load the class and thus can't tell if it's related
            return false;
        }
    }

    static String getFullStackTrace(Throwable e) {
        StringBuilder sb = new StringBuilder();
        
        // Process the exception chain recursively
        appendExceptionDetails(sb, e, null);
        
        return sb.toString();
    }
    
    private static void appendExceptionDetails(StringBuilder sb, Throwable ex, StackTraceElement[] parentStackTrace) {
        if (ex == null) {
            return;
        }

        // Add the exception class name and message
        sb.append(ex.getClass().getName());
        String message = ex.getMessage();
        if (message != null) {
            sb.append(": ").append(message);
        }
        sb.append(System.lineSeparator());
        
        // Add the stack trace elements
        StackTraceElement[] currentStackTrace = ex.getStackTrace();
        int framesInCommon = 0;
        if (parentStackTrace != null) {
            framesInCommon = countCommonFrames(currentStackTrace, parentStackTrace);
        }
        
        int framesToPrint = currentStackTrace.length - framesInCommon;
        for (int i = 0; i < framesToPrint; i++) {
            sb.append("\tat ").append(currentStackTrace[i].toString()).append(System.lineSeparator());
        }
        
        if (framesInCommon > 0) {
            sb.append("\t... ").append(framesInCommon).append(" more").append(System.lineSeparator());
        }
        
        // Handle any suppressed exceptions
        Throwable[] suppressed = ex.getSuppressed();
        if (suppressed != null && suppressed.length > 0) {
            for (Throwable s : suppressed) {
                sb.append("\tSuppressed: ");
                appendExceptionDetails(sb, s, currentStackTrace);
            }
        }
        
        // Handle cause (inner exception)
        Throwable cause = ex.getCause();
        if (cause != null && cause != ex) { // Avoid infinite recursion
            sb.append("Caused by: ");
            appendExceptionDetails(sb, cause, currentStackTrace);
        }
    }
    
    /**
     * Count frames in common between two stack traces, starting from the end.
     * This helps produce more concise stack traces for chained exceptions.
     */
    private static int countCommonFrames(StackTraceElement[] trace1, StackTraceElement[] trace2) {
        int m = trace1.length - 1;
        int n = trace2.length - 1;
        int count = 0;
        while (m >= 0 && n >= 0 && trace1[m].equals(trace2[n])) {
            m--;
            n--;
            count++;
        }
        return count;
    }

    TaskFailureDetails toProto() {
        return TaskFailureDetails.newBuilder()
                .setErrorType(this.getErrorType())
                .setErrorMessage(this.getErrorMessage())
                .setStackTrace(StringValue.of(this.getStackTrace() != null ? this.getStackTrace() : ""))
                .build();
    }
}