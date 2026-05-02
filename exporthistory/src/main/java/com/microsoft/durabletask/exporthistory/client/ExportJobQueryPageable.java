// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.exporthistory.models.ExportJobDescription;
import com.microsoft.durabletask.exporthistory.models.ExportJobQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Auto-paginating iterable over export job descriptions.
 * Mirrors the existing {@code EntityQueryPageable} pattern in the Java SDK.
 */
public class ExportJobQueryPageable implements Iterable<ExportJobDescription> {

    private final ExportJobQuery baseQuery;
    private final java.util.function.Function<ExportJobQuery, ExportJobQueryResult> queryExecutor;

    public ExportJobQueryPageable(
            @Nullable ExportJobQuery baseQuery,
            @Nonnull java.util.function.Function<ExportJobQuery, ExportJobQueryResult> queryExecutor) {
        this.baseQuery = baseQuery;
        this.queryExecutor = queryExecutor;
    }

    @Override
    @Nonnull
    public Iterator<ExportJobDescription> iterator() {
        return new Iterator<ExportJobDescription>() {
            private Iterator<ExportJobDescription> currentPage = Collections.emptyIterator();
            private String continuationToken = baseQuery != null ? baseQuery.getContinuationToken() : null;
            private boolean hasMorePages = true;

            @Override
            public boolean hasNext() {
                if (this.currentPage.hasNext()) {
                    return true;
                }
                if (!this.hasMorePages) {
                    return false;
                }
                fetchNextPage();
                return this.currentPage.hasNext();
            }

            @Override
            public ExportJobDescription next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                return this.currentPage.next();
            }

            private void fetchNextPage() {
                ExportJobQuery query = new ExportJobQuery(
                        baseQuery != null ? baseQuery.getStatus() : null,
                        baseQuery != null ? baseQuery.getJobIdPrefix() : null,
                        baseQuery != null ? baseQuery.getCreatedFrom() : null,
                        baseQuery != null ? baseQuery.getCreatedTo() : null,
                        baseQuery != null ? baseQuery.getPageSize() : null,
                        this.continuationToken);

                ExportJobQueryResult result = queryExecutor.apply(query);
                this.currentPage = result.getJobs().iterator();
                this.continuationToken = result.getContinuationToken();
                this.hasMorePages = this.continuationToken != null && !this.continuationToken.isEmpty();
            }
        };
    }

    /**
     * Provides page-by-page iteration.
     */
    @Nonnull
    public Iterable<ExportJobQueryResult> byPage() {
        return () -> new Iterator<ExportJobQueryResult>() {
            private String continuationToken = baseQuery != null ? baseQuery.getContinuationToken() : null;
            private boolean hasMorePages = true;
            private boolean firstPage = true;

            @Override
            public boolean hasNext() {
                return this.hasMorePages;
            }

            @Override
            public ExportJobQueryResult next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                ExportJobQuery query = new ExportJobQuery(
                        baseQuery != null ? baseQuery.getStatus() : null,
                        baseQuery != null ? baseQuery.getJobIdPrefix() : null,
                        baseQuery != null ? baseQuery.getCreatedFrom() : null,
                        baseQuery != null ? baseQuery.getCreatedTo() : null,
                        baseQuery != null ? baseQuery.getPageSize() : null,
                        this.continuationToken);

                ExportJobQueryResult result = queryExecutor.apply(query);
                this.continuationToken = result.getContinuationToken();
                this.hasMorePages = this.continuationToken != null && !this.continuationToken.isEmpty();
                this.firstPage = false;
                return result;
            }
        };
    }

    /**
     * A single page of export job query results.
     */
    public static class ExportJobQueryResult {
        private final List<ExportJobDescription> jobs;
        private final String continuationToken;

        public ExportJobQueryResult(
                @Nonnull List<ExportJobDescription> jobs,
                @Nullable String continuationToken) {
            this.jobs = jobs;
            this.continuationToken = continuationToken;
        }

        @Nonnull
        public List<ExportJobDescription> getJobs() { return this.jobs; }

        @Nullable
        public String getContinuationToken() { return this.continuationToken; }
    }
}
