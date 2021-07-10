// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class TaskFailedException extends Exception {
    private final int taskId;
    private final String reason;

    TaskFailedException(int taskId, String reason) {
        super(String.format("Task with ID %d failed: %s", taskId, reason));
        this.taskId = taskId;
        this.reason = reason;
    }

    public int getTaskId() {
        return this.taskId;
    }

    public String getReason() {
        return this.reason;
    }
}
