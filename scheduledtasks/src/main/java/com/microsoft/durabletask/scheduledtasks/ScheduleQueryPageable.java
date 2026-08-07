// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * An auto-paginating iterable over schedule descriptions.
 * <p>
 * Iterating with {@link #iterator()} fetches pages on demand and yields individual {@link ScheduleDescription}
 * items. Because status and creation-time filters are applied client-side, a fetched page may be empty even when
 * more matching schedules exist on later pages; the item iterator therefore continues across empty pages until the
 * continuation token is exhausted. Use {@link #byPage()} to iterate one {@link ScheduleQueryResult} at a time.
 */
public final class ScheduleQueryPageable implements Iterable<ScheduleDescription> {

    private final String initialContinuationToken;
    private final Function<String, ScheduleQueryResult> pageFetcher;

    ScheduleQueryPageable(String initialContinuationToken, Function<String, ScheduleQueryResult> pageFetcher) {
        this.initialContinuationToken = initialContinuationToken;
        this.pageFetcher = pageFetcher;
    }

    @Override
    public Iterator<ScheduleDescription> iterator() {
        return new ItemIterator();
    }

    /**
     * Returns an iterable over pages of results. Each page may be underfilled or empty while still advancing the
     * continuation token.
     *
     * @return an iterable over result pages
     */
    public Iterable<ScheduleQueryResult> byPage() {
        return PageIterator::new;
    }

    private final class ItemIterator implements Iterator<ScheduleDescription> {
        private String continuationToken = ScheduleQueryPageable.this.initialContinuationToken;
        private Iterator<ScheduleDescription> currentPage;
        private boolean finished;

        @Override
        public boolean hasNext() {
            while (true) {
                if (this.currentPage != null && this.currentPage.hasNext()) {
                    return true;
                }
                if (this.finished) {
                    return false;
                }
                fetchNextPage();
            }
        }

        @Override
        public ScheduleDescription next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return this.currentPage.next();
        }

        private void fetchNextPage() {
            ScheduleQueryResult result = ScheduleQueryPageable.this.pageFetcher.apply(this.continuationToken);
            this.continuationToken = result.getContinuationToken();
            if (this.continuationToken == null || this.continuationToken.isEmpty()) {
                this.finished = true;
            }
            List<ScheduleDescription> items = result.getDescriptions();
            this.currentPage = items.iterator();
        }
    }

    private final class PageIterator implements Iterator<ScheduleQueryResult> {
        private String continuationToken = ScheduleQueryPageable.this.initialContinuationToken;
        private boolean finished;

        @Override
        public boolean hasNext() {
            return !this.finished;
        }

        @Override
        public ScheduleQueryResult next() {
            if (this.finished) {
                throw new NoSuchElementException();
            }
            ScheduleQueryResult result = ScheduleQueryPageable.this.pageFetcher.apply(this.continuationToken);
            this.continuationToken = result.getContinuationToken();
            if (this.continuationToken == null || this.continuationToken.isEmpty()) {
                this.finished = true;
            }
            return result;
        }
    }
}
