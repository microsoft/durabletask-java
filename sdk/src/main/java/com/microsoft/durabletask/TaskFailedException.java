// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class TaskFailedException extends Exception {
    private final FailureDetails details;
    private final String taskName;
    private final int taskId;

    TaskFailedException(String taskName, int taskId, FailureDetails details) {
        this(getExceptionMessage(taskName, taskId, details), taskName, taskId, details);
    }

    protected TaskFailedException(String message, String taskName, int taskId, FailureDetails details) {
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
        return this.details.getErrorType();
    }

    public String getExceptionMessage() {
        return this.details.getErrorMessage();
    }

    public String getExceptionDetails() {
        return this.details.getStackTrace();
    }

    FailureDetails getErrorDetails() {
        return this.details;
    }

    private static String getExceptionMessage(String taskName, int taskId, FailureDetails details) {
        return String.format("Task '%s' (#%d) failed with an unhandled exception: %s",
                taskName,
                taskId,
                details.getErrorMessage());
    }
}
