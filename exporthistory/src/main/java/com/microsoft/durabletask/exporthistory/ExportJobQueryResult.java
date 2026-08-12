// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Result of {@link ExportHistoryClient#listJobs(ExportJobQuery)}: a page of export job descriptions and a
 * continuation token for the next page.
 */
public final class ExportJobQueryResult {

    private final List<ExportJobDescription> jobs;
    private final String continuationToken;

    /**
     * Creates an {@code ExportJobQueryResult}.
     *
     * @param jobs              the page of export job descriptions
     * @param continuationToken the continuation token for the next page, or {@code null}
     */
    public ExportJobQueryResult(List<ExportJobDescription> jobs, @Nullable String continuationToken) {
        this.jobs = Collections.unmodifiableList(jobs);
        this.continuationToken = continuationToken;
    }

    /** @return an unmodifiable page of export job descriptions. */
    public List<ExportJobDescription> getJobs() {
        return this.jobs;
    }

    /** @return the continuation token for the next page, or {@code null} if there are no more results. */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
