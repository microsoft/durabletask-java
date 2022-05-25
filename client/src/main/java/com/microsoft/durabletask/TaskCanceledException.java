// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

// TODO: This should inherit from Exception, not TaskFailedException
/**
 * Represents a task cancellation, either because of a timeout or because of an explicit cancellation operation.
 */
public final class TaskCanceledException extends TaskFailedException {
    // Only intended to be created within this package
    TaskCanceledException(String message, String taskName, int taskId) {
        super(message, taskName, taskId, new FailureDetails(TaskCanceledException.class.getName(), message, "", true));
    }
}
