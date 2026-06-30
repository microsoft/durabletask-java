// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;

import java.util.Objects;

/**
 * Client-side registration for the export history feature.
 * <p>
 * Builds a {@link DurableTaskClient} from the given builder and returns an {@link ExportHistoryClient} bound to the
 * supplied blob storage destination. Mirrors the .NET client extension.
 */
public final class ExportHistoryClientExtensions {

    private ExportHistoryClientExtensions() {
    }

    /**
     * Enables export history on the given client builder and returns an {@link ExportHistoryClient}.
     *
     * @param builder the client builder to build from
     * @param storage the blob storage destination options
     * @return an export history client bound to the destination
     */
    public static ExportHistoryClient useExportHistory(
            DurableTaskGrpcClientBuilder builder,
            ExportHistoryStorageOptions storage) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(storage, "storage must not be null");
        DurableTaskClient client = builder.build();
        return new ExportHistoryClient(client, storage);
    }

    /**
     * Returns an {@link ExportHistoryClient} bound to an existing {@link DurableTaskClient}.
     *
     * @param client  an existing Durable Task client
     * @param storage the blob storage destination options
     * @return an export history client bound to the destination
     */
    public static ExportHistoryClient useExportHistory(
            DurableTaskClient client,
            ExportHistoryStorageOptions storage) {
        Objects.requireNonNull(client, "client must not be null");
        Objects.requireNonNull(storage, "storage must not be null");
        return new ExportHistoryClient(client, storage);
    }
}
