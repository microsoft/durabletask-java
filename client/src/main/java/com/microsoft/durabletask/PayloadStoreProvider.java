// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Service provider interface for discovering {@link PayloadStore} implementations at runtime.
 * <p>
 * Implementations are discovered via {@link java.util.ServiceLoader}. To register a provider,
 * create a file {@code META-INF/services/com.microsoft.durabletask.PayloadStoreProvider}
 * containing the fully qualified class name of the implementation.
 * <p>
 * The provider is responsible for reading its own configuration (e.g., environment variables)
 * and determining whether it can create a functional {@link PayloadStore}.
 *
 * @see PayloadStore
 */
public interface PayloadStoreProvider {

    /**
     * Attempts to create a {@link PayloadStore} based on available configuration.
     * <p>
     * Implementations should inspect environment variables or other configuration sources
     * to determine if they can provide a store. If the required configuration is not present,
     * this method should return {@code null} rather than throwing an exception.
     *
     * @return a configured {@link PayloadStore}, or {@code null} if the required configuration
     *         is not available
     */
    PayloadStore create();
}
