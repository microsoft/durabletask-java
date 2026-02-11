// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Represents the result of an entity query operation, including the matching entities and
 * an optional continuation token for pagination.
 */
public final class EntityQueryResult {
    private final List<EntityMetadata> entities;
    private final String continuationToken;

    /**
     * Creates a new {@code EntityQueryResult}.
     *
     * @param entities          the list of entity metadata records matching the query
     * @param continuationToken the continuation token for fetching the next page, or {@code null} if no more results
     */
    EntityQueryResult(List<EntityMetadata> entities, @Nullable String continuationToken) {
        this.entities = entities;
        this.continuationToken = continuationToken;
    }

    /**
     * Gets the list of entity metadata records matching the query.
     *
     * @return the list of entity metadata
     */
    public List<EntityMetadata> getEntities() {
        return this.entities;
    }

    /**
     * Gets the continuation token for fetching the next page of results.
     *
     * @return the continuation token, or {@code null} if there are no more results
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
