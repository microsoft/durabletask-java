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

    public FailureDetails(
            String errorType,
            @Nullable String errorMessage,
            @Nullable String errorDetails) {
        this.errorType = errorType;
        this.stackTrace = errorDetails;

        // Error message can be null for things like NullPointerException but the gRPC contract doesn't allow null
        this.errorMessage = Objects.requireNonNullElse(errorMessage, "");
    }

    public FailureDetails(Exception exception) {
        this(exception.getClass().getName(), exception.getMessage(), getFullStackTrace(exception));
    }

    public FailureDetails(TaskFailureDetails proto) {
        this.errorType = proto.getErrorType();
        this.errorMessage = proto.getErrorMessage();
        this.stackTrace = proto.getStackTrace().getValue();
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