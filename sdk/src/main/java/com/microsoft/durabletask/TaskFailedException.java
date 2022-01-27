// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class TaskFailedException extends Exception {
    private final ErrorDetails details;
    private final String taskName;
    private final int taskId;

    TaskFailedException(String message, String taskName, int taskId, ErrorDetails details) {
        super(message);
        this.taskName = taskName;
        this.taskId = taskId;
        this.details = details;
    }

    public int getTaskId() {
        return this.taskId;
    }

    public String getTaskName() {
        return this.taskName;
    }

    public String getExceptionName() {
        return this.details.getErrorName();
    }

    public String getExceptionMessage() {
        return this.details.getErrorMessage();
    }

    public String getExceptionDetails() {
        return this.details.getErrorDetails();
    }

    ErrorDetails getErrorDetails() {
        return this.details;
    }
}
