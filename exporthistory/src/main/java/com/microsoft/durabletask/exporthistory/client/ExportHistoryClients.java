// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;

import javax.annotation.Nonnull;

/**
 * Factory helper for creating {@link ExportHistoryClient} instances.
 * <p>
 * Lives in the exporthistory module (not client) to preserve module layering.
 */
public final class ExportHistoryClients {

    private ExportHistoryClients() {
    }

    /**
     * Creates a new {@link ExportHistoryClient} backed by the specified {@link DurableTaskClient}.
     *
     * @param durableTaskClient the durable task client for entity/orchestration operations
     * @param options           the Azure Blob Storage configuration for exports
     * @return a new export history client
     */
    @Nonnull
    public static ExportHistoryClient create(
            @Nonnull DurableTaskClient durableTaskClient,
            @Nonnull ExportHistoryStorageOptions options) {
        return new DefaultExportHistoryClient(durableTaskClient, options);
    }
}
