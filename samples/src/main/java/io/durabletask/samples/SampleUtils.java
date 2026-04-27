// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;

/**
 * Helpers for the standalone entity samples.
 * <p>
 * By default, these samples connect to a plain Durable Task sidecar on {@code localhost:4001}.
 * When one of the following environment variables is set, they instead connect to a
 * Durable Task Scheduler (e.g. the DTS emulator), which requires a task hub header:
 * <ul>
 *   <li>{@code DURABLE_TASK_CONNECTION_STRING} — full connection string, used as-is.</li>
 *   <li>{@code ENDPOINT} + optional {@code TASKHUB} — constructs a connection string
 *       (authentication defaults to {@code None} for localhost, {@code DefaultAzure} otherwise).</li>
 * </ul>
 */
final class SampleUtils {
    private SampleUtils() {}

    static DurableTaskGrpcWorkerBuilder newWorkerBuilder() {
        String cs = connectionString();
        return cs != null
                ? DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(cs)
                : new DurableTaskGrpcWorkerBuilder();
    }

    static DurableTaskGrpcClientBuilder newClientBuilder() {
        String cs = connectionString();
        return cs != null
                ? DurableTaskSchedulerClientExtensions.createClientBuilder(cs)
                : new DurableTaskGrpcClientBuilder();
    }

    private static String connectionString() {
        String cs = System.getenv("DURABLE_TASK_CONNECTION_STRING");
        if (cs != null && !cs.isEmpty()) {
            return cs;
        }
        String endpoint = System.getenv("ENDPOINT");
        if (endpoint == null || endpoint.isEmpty()) {
            return null;
        }
        String taskHub = System.getenv("TASKHUB");
        if (taskHub == null || taskHub.isEmpty()) {
            taskHub = "default";
        }
        String host = endpoint.replaceFirst("^https?://", "");
        String authType = host.startsWith("localhost") || host.startsWith("127.")
                ? "None" : "DefaultAzure";
        return String.format("Endpoint=%s;TaskHub=%s;Authentication=%s", endpoint, taskHub, authType);
    }
}
