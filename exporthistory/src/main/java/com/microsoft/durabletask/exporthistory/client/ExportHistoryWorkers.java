// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.exporthistory.activities.ExportInstanceHistoryActivityFactory;
import com.microsoft.durabletask.exporthistory.activities.ListTerminalInstancesActivityFactory;
import com.microsoft.durabletask.exporthistory.entity.ExportJob;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;
import com.microsoft.durabletask.exporthistory.orchestrations.ExecuteExportJobOperationOrchestratorFactory;
import com.microsoft.durabletask.exporthistory.orchestrations.ExportJobOrchestratorFactory;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Static helper for registering export history components on a worker builder.
 * <p>
 * Lives in the exporthistory module (not client) to preserve module layering.
 */
public final class ExportHistoryWorkers {

    private ExportHistoryWorkers() {
    }

    /**
     * Registers all export history components (entity, orchestrators, activities) on the worker builder.
     *
     * @param builder       the worker builder to register on
     * @param client        the durable task client (needed by activity factories)
     * @param options       the Azure Blob Storage configuration for exports
     */
    public static void register(
            @Nonnull DurableTaskGrpcWorkerBuilder builder,
            @Nonnull DurableTaskClient client,
            @Nonnull ExportHistoryStorageOptions options) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(options, "options");

        builder.addEntity(ExportJob.class);
        builder.addOrchestration(new ExportJobOrchestratorFactory());
        builder.addOrchestration(new ExecuteExportJobOperationOrchestratorFactory());
        builder.addActivity(new ExportInstanceHistoryActivityFactory(client, options));
        builder.addActivity(new ListTerminalInstancesActivityFactory(client));
    }
}
