// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.microsoft.durabletask.PayloadStore;
import com.microsoft.durabletask.PayloadStoreProvider;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link PayloadStoreProvider} implementation that creates a {@link BlobPayloadStore}
 * when the {@code DURABLETASK_LARGE_PAYLOADS_CONNECTION_STRING} environment variable is set.
 * <p>
 * This provider is discovered automatically via {@link java.util.ServiceLoader}.
 */
public final class BlobPayloadStoreProvider implements PayloadStoreProvider {

    private static final Logger logger = Logger.getLogger(BlobPayloadStoreProvider.class.getName());
    private static final String ENV_STORAGE_CONNECTION_STRING = "DURABLETASK_LARGE_PAYLOADS_CONNECTION_STRING";

    @Override
    public PayloadStore create() {
        String connectionString = System.getenv(ENV_STORAGE_CONNECTION_STRING);
        if (connectionString == null || connectionString.isEmpty()) {
            return null;
        }

        try {
            BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
                .setConnectionString(connectionString)
                .build();
            logger.info("Large payload externalization enabled using Azure Blob Storage");
            return new BlobPayloadStore(options);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                "Failed to initialize BlobPayloadStore; large payloads will not be externalized", e);
            return null;
        }
    }
}
