// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A single page of schedule descriptions returned by a query, along with a continuation token for the next page.
 * <p>
 * Because status and creation-time filters are applied client-side after backend paging, a page may contain fewer
 * descriptions than the requested page size (or none) while still carrying a continuation token.
 */
public final class ScheduleQueryResult {

    private final List<ScheduleDescription> descriptions;
    private final String continuationToken;

    ScheduleQueryResult(List<ScheduleDescription> descriptions, @Nullable String continuationToken) {
        this.descriptions = descriptions == null
                ? Collections.<ScheduleDescription>emptyList()
                : Collections.unmodifiableList(descriptions);
        this.continuationToken = continuationToken;
    }

    /** @return the schedule descriptions on this page. */
    public List<ScheduleDescription> getDescriptions() {
        return this.descriptions;
    }

    /** @return the continuation token for the next page, or {@code null} when there are no more pages. */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
