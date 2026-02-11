// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Represents a query filter for fetching durable entity metadata from the store.
 * <p>
 * Use the builder-style setters to configure the query parameters, then pass this object to
 * {@link DurableTaskClient#queryEntities(EntityQuery)}.
 */
public final class EntityQuery {
    private String instanceIdStartsWith;
    private Instant lastModifiedFrom;
    private Instant lastModifiedTo;
    private boolean includeState;
    private boolean includeTransient;
    private Integer pageSize;
    private String continuationToken;

    /**
     * Creates a new {@code EntityQuery} with default settings.
     */
    public EntityQuery() {
    }

    /**
     * Sets a prefix filter on entity instance IDs.
     *
     * @param instanceIdStartsWith the instance ID prefix to filter by, or {@code null} for no filter
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setInstanceIdStartsWith(@Nullable String instanceIdStartsWith) {
        this.instanceIdStartsWith = instanceIdStartsWith;
        return this;
    }

    /**
     * Gets the instance ID prefix filter.
     *
     * @return the instance ID prefix, or {@code null}
     */
    @Nullable
    public String getInstanceIdStartsWith() {
        return this.instanceIdStartsWith;
    }

    /**
     * Sets the minimum last-modified time filter (inclusive).
     *
     * @param lastModifiedFrom the minimum last-modified time, or {@code null} for no lower bound
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setLastModifiedFrom(@Nullable Instant lastModifiedFrom) {
        this.lastModifiedFrom = lastModifiedFrom;
        return this;
    }

    /**
     * Gets the minimum last-modified time filter.
     *
     * @return the minimum last-modified time, or {@code null}
     */
    @Nullable
    public Instant getLastModifiedFrom() {
        return this.lastModifiedFrom;
    }

    /**
     * Sets the maximum last-modified time filter (inclusive).
     *
     * @param lastModifiedTo the maximum last-modified time, or {@code null} for no upper bound
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setLastModifiedTo(@Nullable Instant lastModifiedTo) {
        this.lastModifiedTo = lastModifiedTo;
        return this;
    }

    /**
     * Gets the maximum last-modified time filter.
     *
     * @return the maximum last-modified time, or {@code null}
     */
    @Nullable
    public Instant getLastModifiedTo() {
        return this.lastModifiedTo;
    }

    /**
     * Sets whether to include entity state in the query results.
     *
     * @param includeState {@code true} to include state, {@code false} to omit it
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setIncludeState(boolean includeState) {
        this.includeState = includeState;
        return this;
    }

    /**
     * Gets whether entity state is included in query results.
     *
     * @return {@code true} if state is included
     */
    public boolean isIncludeState() {
        return this.includeState;
    }

    /**
     * Sets whether to include transient (not yet persisted) entities in the results.
     *
     * @param includeTransient {@code true} to include transient entities
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setIncludeTransient(boolean includeTransient) {
        this.includeTransient = includeTransient;
        return this;
    }

    /**
     * Gets whether transient entities are included in query results.
     *
     * @return {@code true} if transient entities are included
     */
    public boolean isIncludeTransient() {
        return this.includeTransient;
    }

    /**
     * Sets the maximum number of results to return per page.
     *
     * @param pageSize the page size, or {@code null} for the server default
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setPageSize(@Nullable Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Gets the maximum number of results per page.
     *
     * @return the page size, or {@code null} for the server default
     */
    @Nullable
    public Integer getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the continuation token for fetching the next page of results.
     *
     * @param continuationToken the continuation token from a previous query, or {@code null} to start from the beginning
     * @return this {@code EntityQuery} for chaining
     */
    public EntityQuery setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
        return this;
    }

    /**
     * Gets the continuation token for pagination.
     *
     * @return the continuation token, or {@code null}
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
