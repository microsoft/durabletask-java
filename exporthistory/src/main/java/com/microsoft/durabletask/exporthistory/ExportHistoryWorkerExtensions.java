// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityFactory;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationFactory;

import java.util.Objects;

/**
 * Worker-side registration for the export history feature.
 * <p>
 * Registers the {@link ExportJob} entity, the {@link ExportJobOrchestrator} and
 * {@link ExecuteExportJobOperationOrchestrator} orchestrators, and the {@link ListTerminalInstancesActivity} and
 * {@link ExportInstanceHistoryActivity} activities.
 * <p>
 * The export activities require an explicit {@link DurableTaskClient} to call {@code listInstanceIds} /
 * {@code getOrchestrationHistory} against the same backend the worker targets.
 */
public final class ExportHistoryWorkerExtensions {

    private ExportHistoryWorkerExtensions() {
    }

    /**
     * Enables export history on the given worker builder.
     *
     * @param builder           the worker builder to configure
     * @param storage           the blob storage destination options
     * @param durableTaskClient a client connected to the same backend, used by the export activities
     * @return the worker builder, for chaining
     */
    public static DurableTaskGrpcWorkerBuilder useExportHistory(
            DurableTaskGrpcWorkerBuilder builder,
            ExportHistoryStorageOptions storage,
            DurableTaskClient durableTaskClient) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(storage, "storage must not be null");
        Objects.requireNonNull(durableTaskClient, "durableTaskClient must not be null");

        BlobExportWriter writer = new BlobExportWriter(storage);

        builder.addEntity(ExportJob.NAME, ExportJob::new);

        builder.addOrchestration(orchestrationFactory(
                ExportJobOrchestrator.NAME, ExportJobOrchestrator::new));
        builder.addOrchestration(orchestrationFactory(
                ExecuteExportJobOperationOrchestrator.NAME, ExecuteExportJobOperationOrchestrator::new));

        builder.addActivity(activityFactory(
                ListTerminalInstancesActivity.NAME,
                () -> new ListTerminalInstancesActivity(durableTaskClient)));
        builder.addActivity(activityFactory(
                ExportInstanceHistoryActivity.NAME,
                () -> new ExportInstanceHistoryActivity(durableTaskClient, writer)));

        return builder;
    }

    private static TaskOrchestrationFactory orchestrationFactory(
            String name, java.util.function.Supplier<TaskOrchestration> supplier) {
        return new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public TaskOrchestration create() {
                return supplier.get();
            }
        };
    }

    private static TaskActivityFactory activityFactory(
            String name, java.util.function.Supplier<TaskActivity> supplier) {
        return new TaskActivityFactory() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public TaskActivity create() {
                return supplier.get();
            }
        };
    }
}
