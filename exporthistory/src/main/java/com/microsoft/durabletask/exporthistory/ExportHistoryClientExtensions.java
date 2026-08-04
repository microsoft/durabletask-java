// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;

import java.util.Objects;

/**
 * Client-side registration for the export history feature.
 * <p>
 * Returns an {@link ExportHistoryClient} bound to a caller-owned {@link DurableTaskClient} and the supplied blob
 * storage destination. The caller retains ownership of the client's lifecycle (gRPC channel).
 */
public final class ExportHistoryClientExtensions {

    private ExportHistoryClientExtensions() {
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
