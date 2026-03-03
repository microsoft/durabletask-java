// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * An auto-paginating iterable over entity query results.
 * <p>
 * This class automatically handles pagination when iterating over entity metadata results.
 * It fetches pages from the store on demand and yields individual {@link EntityMetadata}
 * items to the caller.
 * <p>
 * Use {@link DurableTaskClient#getEntities(EntityQuery)} to obtain an instance of this class.
 *
 * <h3>Example: iterate over all entities</h3>
 * <pre>{@code
 * EntityQuery query = new EntityQuery()
 *     .setInstanceIdStartsWith("counter")
 *     .setIncludeState(true);
 *
 * for (EntityMetadata entity : client.getEntities(query)) {
 *     System.out.println(entity.getEntityInstanceId());
 * }
 * }</pre>
 *
 * <h3>Example: iterate page by page</h3>
 * <pre>{@code
 * for (EntityQueryResult page : client.getEntities(query).byPage()) {
 *     System.out.println("Got " + page.getEntities().size() + " entities");
 *     for (EntityMetadata entity : page.getEntities()) {
 *         System.out.println(entity.getEntityInstanceId());
 *     }
 * }
 * }</pre>
 */
public final class EntityQueryPageable implements Iterable<EntityMetadata> {
    private final EntityQuery baseQuery;
    private final Function<EntityQuery, EntityQueryResult> queryExecutor;

    /**
     * Creates a new {@code EntityQueryPageable}.
     *
     * @param baseQuery     the base query parameters
     * @param queryExecutor the function that executes a single page query
     */
    EntityQueryPageable(EntityQuery baseQuery, Function<EntityQuery, EntityQueryResult> queryExecutor) {
        this.baseQuery = baseQuery;
        this.queryExecutor = queryExecutor;
    }

    /**
     * Returns an iterator over individual {@link EntityMetadata} items, automatically
     * fetching subsequent pages as needed.
     *
     * @return an iterator over all matching entities
     */
    @Override
    public Iterator<EntityMetadata> iterator() {
        return new EntityItemIterator();
    }

    /**
     * Returns an iterable over pages of results, where each page is an {@link EntityQueryResult}
     * containing a list of entities and an optional continuation token.
     *
     * @return an iterable over result pages
     */
    public Iterable<EntityQueryResult> byPage() {
        return PageIterable::new;
    }

    private class EntityItemIterator implements Iterator<EntityMetadata> {
        private String continuationToken = baseQuery.getContinuationToken();
        private Iterator<EntityMetadata> currentPageIterator;
        private boolean finished;

        EntityItemIterator() {
            fetchNextPage();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentPageIterator != null && currentPageIterator.hasNext()) {
                    return true;
                }
                if (finished) {
                    return false;
                }
                fetchNextPage();
            }
        }

        @Override
        public EntityMetadata next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentPageIterator.next();
        }

        private void fetchNextPage() {
            if (finished) {
                return;
            }

            EntityQuery pageQuery = cloneQuery(baseQuery);
            pageQuery.setContinuationToken(continuationToken);

            EntityQueryResult result = queryExecutor.apply(pageQuery);
            List<EntityMetadata> entities = result.getEntities();

            if (entities == null || entities.isEmpty()) {
                finished = true;
                currentPageIterator = null;
                return;
            }

            currentPageIterator = entities.iterator();
            continuationToken = result.getContinuationToken();

            if (continuationToken == null || continuationToken.isEmpty()) {
                finished = true;
            }
        }
    }

    private class PageIterable implements Iterator<EntityQueryResult> {
        private String continuationToken = baseQuery.getContinuationToken();
        private boolean finished;
        private boolean firstPage = true;

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public EntityQueryResult next() {
            if (finished) {
                throw new NoSuchElementException();
            }

            EntityQuery pageQuery = cloneQuery(baseQuery);
            if (!firstPage) {
                pageQuery.setContinuationToken(continuationToken);
            }
            firstPage = false;

            EntityQueryResult result = queryExecutor.apply(pageQuery);
            continuationToken = result.getContinuationToken();

            if (continuationToken == null || continuationToken.isEmpty()) {
                finished = true;
            }

            return result;
        }
    }

    private static EntityQuery cloneQuery(EntityQuery source) {
        EntityQuery clone = new EntityQuery();
        if (source.getInstanceIdStartsWith() != null) {
            // Use raw setter value since the source is already normalized
            clone.setInstanceIdStartsWith(source.getInstanceIdStartsWith());
        }
        clone.setLastModifiedFrom(source.getLastModifiedFrom());
        clone.setLastModifiedTo(source.getLastModifiedTo());
        clone.setIncludeState(source.isIncludeState());
        clone.setIncludeTransient(source.isIncludeTransient());
        clone.setPageSize(source.getPageSize());
        clone.setContinuationToken(source.getContinuationToken());
        return clone;
    }
}
