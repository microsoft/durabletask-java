// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Options for configuring versioning behavior in the DurableTaskGrpcWorker.
 */
public final class DurableTaskGrpcWorkerVersioningOptions {

    /**
     * Strategy for matching versions.
     * NONE: No version matching is performed.
     * STRICT: The version must match exactly.
     * CURRENTOROLDER: The version must be the current version or older.
     */
    public enum VersionMatchStrategy {
        NONE,
        STRICT,
        CURRENTOROLDER;
    }

    /**
     * Strategy for handling version mismatches.
     * REJECT: Reject the orchestration if the version does not match. The orchestration will be retried later.
     * FAIL: Fail the orchestration if the version does not match.
     */
    public enum VersionFailureStrategy {
        REJECT,
        FAIL;
    }

    private final String version;
    private final String defaultVersion;
    private final VersionMatchStrategy matchStrategy;
    private final VersionFailureStrategy failureStrategy;

    /**
     * Constructor for DurableTaskGrpcWorkerVersioningOptions.
     * @param version the version that is matched against orchestrations
     * @param defaultVersion the default version used when starting sub orchestrations from this worker
     * @param matchStrategy the strategy for matching versions
     * @param failureStrategy the strategy for handling version mismatches
     */
    public DurableTaskGrpcWorkerVersioningOptions(String version, String defaultVersion, VersionMatchStrategy matchStrategy, VersionFailureStrategy failureStrategy) {
        this.version = version;
        this.defaultVersion = defaultVersion;
        this.matchStrategy = matchStrategy;
        this.failureStrategy = failureStrategy;
    }

    /**
     * Gets the version that is matched against orchestrations.
     * @return the version that is matched against orchestrations
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the default version used when starting sub orchestrations from this worker.
     * @return the default version used when starting sub orchestrations from this worker
     */
    public String getDefaultVersion() {
        return defaultVersion;
    }

    /**
     * Gets the strategy for matching versions.
     * @return the strategy for matching versions
     */
    public VersionMatchStrategy getMatchStrategy() {
        return matchStrategy;
    }

    /**
     * Gets the strategy for handling version mismatches.
     * @return the strategy for handling version mismatches
     */
    public VersionFailureStrategy getFailureStrategy() {
        return failureStrategy;
    }
}