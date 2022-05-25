// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TaskFailureDetails;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Class that represents the details of a task failure.
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
        this(exception.getClass().getName(), exception.getMessage(), getFullStackTrace(exception), false);
    }

    FailureDetails(TaskFailureDetails proto) {
        this(proto.getErrorType(),
             proto.getErrorMessage(),
             proto.getStackTrace().getValue(),
             proto.getIsNonRetriable());
    }

    /**
     * Gets the exception class name.
     * @return the exception class name
     */
    @Nonnull
    public String getErrorType() {
        return this.errorType;
    }

    /**
     * Gets a summary description of the error. If the failure was caused by an exception, the exception message
     * is returned.
     *
     * @return a summary description of the error
     */
    @Nonnull
    public String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Gets the stack trace of the failure exception.
     * @return the stack trace of the failure exception
     */
    @Nullable
    public String getStackTrace() {
        return this.stackTrace;
    }

    /**
     * Returns {@code true} if the exception is cannot be retried, otherwise {@code false}.
     * @return {@code true} if the exception is cannot be retried, otherwise {@code false}.
     */
    public boolean getIsNonRetriable() {
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
        StackTraceElement[] elements = e.getStackTrace();

        // Plan for 256 characters per stack frame (which is likely on the high-end)
        StringBuilder sb = new StringBuilder(elements.length * 256);
        for (StackTraceElement element : elements) {
            sb.append("\tat ").append(element.toString()).append(System.lineSeparator());
        }
        return sb.toString();
    }

    TaskFailureDetails toProto() {
        return TaskFailureDetails.newBuilder()
                .setErrorType(this.getErrorType())
                .setErrorMessage(this.getErrorMessage())
                .setStackTrace(StringValue.of(this.getStackTrace() != null ? this.getStackTrace() : ""))
                .build();
    }
}