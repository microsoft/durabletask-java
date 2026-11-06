// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;

/**
 * Options for restarting an orchestration with a required target instance ID and an optional version.
 */
public final class RestartInstanceOptions {
    private String newInstanceId;
    private String version;

    /**
     * Creates a new {@link RestartInstanceOptions}. A target instance ID must be set before restarting.
     */
    public RestartInstanceOptions() {
    }

    /**
     * Sets the required target instance ID.
     *
     * @param newInstanceId the nonblank ID of the restarted orchestration
     * @return this {@link RestartInstanceOptions} object
     */
    public RestartInstanceOptions setNewInstanceId(String newInstanceId) {
        this.newInstanceId = newInstanceId;
        return this;
    }

    /**
     * Gets the target instance ID.
     *
     * @return the target instance ID, or {@code null} if none has been set
     */
    @Nullable
    public String getNewInstanceId() {
        return this.newInstanceId;
    }

    /**
     * Sets the version override. A {@code null} value preserves the backend's default restart behavior;
     * an empty string explicitly selects an unversioned orchestration.
     *
     * @param version the version override
     * @return this {@link RestartInstanceOptions} object
     */
    public RestartInstanceOptions setVersion(@Nullable String version) {
        this.version = version;
        return this;
    }

    /**
     * Gets the version override.
     *
     * @return the version override, or {@code null} to preserve the backend's default restart behavior
     */
    @Nullable
    public String getVersion() {
        return this.version;
    }
}
