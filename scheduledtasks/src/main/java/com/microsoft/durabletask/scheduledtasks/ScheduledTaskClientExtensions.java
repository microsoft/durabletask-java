// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.DurableTaskClient;

import java.util.Objects;

/**
 * Client-side registration for the scheduled-tasks feature.
 * <p>
 * Returns a {@link ScheduledTaskClient} bound to a caller-owned {@link DurableTaskClient}. The caller retains
 * ownership of the client's lifecycle (gRPC channel).
 */
public final class ScheduledTaskClientExtensions {

    private ScheduledTaskClientExtensions() {
    }

    /**
     * Returns a {@link ScheduledTaskClient} bound to an existing {@link DurableTaskClient}.
     *
     * @param client an existing Durable Task client connected to the same task hub as the worker
     * @return a scheduled-task client
     */
    public static ScheduledTaskClient useScheduledTasks(DurableTaskClient client) {
        Objects.requireNonNull(client, "client must not be null");
        return new ScheduledTaskClient(client);
    }
}
