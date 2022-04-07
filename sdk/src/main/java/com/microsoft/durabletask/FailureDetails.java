// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.protobuf.OrchestratorService.TaskFailureDetails;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

class FailureDetails {
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
        this.errorMessage = Objects.requireNonNullElse(errorMessage, "");
        this.isNonRetriable = isNonRetriable;
    }

    public FailureDetails(Exception exception) {
        this(exception.getClass().getName(), exception.getMessage(), getFullStackTrace(exception), false);
    }

    public FailureDetails(TaskFailureDetails proto) {
        this(proto.getErrorType(),
             proto.getErrorMessage(),
             proto.getStackTrace().getValue(),
             proto.getIsNonRetriable());
    }

    @Nonnull
    public String getErrorType() {
        return this.errorType;
    }

    @Nonnull
    public String getErrorMessage() {
        return this.errorMessage;
    }

    @Nullable
    public String getStackTrace() {
        return this.stackTrace;
    }

    public boolean getIsNonRetriable() {
        return this.isNonRetriable;
    }

    public boolean isCausedBy(Class<? extends Exception> exceptionClass) {
        // First, try comparing the class names
        String expectedClassName = exceptionClass.getName();
        String actualClassName = this.getErrorType();
        if (expectedClassName.equalsIgnoreCase(actualClassName)) {
            return true;
        }

        try {
            // Next, try using reflection to load the failure's class type and see if it's
            // a sub-type of the specified exception. For example, this should always succeed
            // if exceptionClass is System.Exception.
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
                .setStackTrace(StringValue.of(Objects.requireNonNullElse(this.getStackTrace(), "")))
                .build();
    }
}