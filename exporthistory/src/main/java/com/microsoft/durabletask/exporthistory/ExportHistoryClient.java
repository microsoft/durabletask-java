// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityMetadata;
import com.microsoft.durabletask.EntityQuery;
import com.microsoft.durabletask.EntityQueryResult;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Convenience client for creating, reading, and listing export jobs, backed by entity operations and reads.
 * <p>
 * Obtain an instance via {@link ExportHistoryClientExtensions#useExportHistory}.
 */
public final class ExportHistoryClient {

    private static final String ENTITY_ID_PREFIX = "@" + ExportJob.NAME.toLowerCase(java.util.Locale.ROOT) + "@";

    private final DurableTaskClient durableTaskClient;
    private final ExportHistoryStorageOptions storageOptions;

    ExportHistoryClient(DurableTaskClient durableTaskClient, ExportHistoryStorageOptions storageOptions) {
        this.durableTaskClient = durableTaskClient;
        this.storageOptions = storageOptions;
    }

    /**
     * Creates a new export job and returns a client bound to it.
     *
     * @param options the creation options
     * @return a job client bound to the created job
     */
    public ExportHistoryJobClient createJob(ExportJobCreationOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }
        ExportHistoryJobClient jobClient = this.getJobClient(options.getJobId());
        jobClient.create(options);
        return jobClient;
    }

    /**
     * Gets the description of an export job.
     *
     * @param jobId the export job ID
     * @return the export job description
     * @throws ExportJobNotFoundException if the job does not exist
     */
    public ExportJobDescription getJob(String jobId) {
        return this.getJobClient(jobId).describe();
    }

    /**
     * Gets a job client bound to the specified job ID without creating it.
     *
     * @param jobId the export job ID
     * @return a job client
     */
    public ExportHistoryJobClient getJobClient(String jobId) {
        return new ExportHistoryJobClient(this.durableTaskClient, jobId, this.storageOptions);
    }

    /**
     * Lists export jobs matching the query (single page).
     *
     * @param filter the query, or {@code null} for defaults
     * @return a page of matching export job descriptions and a continuation token
     */
    public ExportJobQueryResult listJobs(@Nullable ExportJobQuery filter) {
        ExportJobQuery query = filter == null ? new ExportJobQuery() : filter;
        String prefix = query.getJobIdPrefix() == null ? "" : query.getJobIdPrefix();
        int pageSize = query.getPageSize() == null ? ExportJobQuery.DEFAULT_PAGE_SIZE : query.getPageSize();

        EntityQuery entityQuery = new EntityQuery()
                .setInstanceIdStartsWith(ENTITY_ID_PREFIX + prefix)
                .setIncludeState(true)
                .setPageSize(pageSize)
                .setContinuationToken(query.getContinuationToken());

        EntityQueryResult result = this.durableTaskClient.getEntities().queryEntities(entityQuery);

        List<ExportJobDescription> jobs = new ArrayList<>();
        for (EntityMetadata metadata : result.getEntities()) {
            ExportJobState state = metadata.readStateAs(ExportJobState.class);
            if (state == null || !matchesFilter(state, query)) {
                continue;
            }
            jobs.add(ExportJobDescription.fromState(metadata.getEntityInstanceId().getKey(), state));
        }

        return new ExportJobQueryResult(jobs, result.getContinuationToken());
    }

    private static boolean matchesFilter(ExportJobState state, ExportJobQuery filter) {
        if (filter.getStatus() != null && state.getStatus() != filter.getStatus()) {
            return false;
        }
        Instant createdAt = state.getCreatedAt();
        if (filter.getCreatedFrom() != null
                && (createdAt == null || !createdAt.isAfter(filter.getCreatedFrom()))) {
            return false;
        }
        if (filter.getCreatedTo() != null
                && (createdAt == null || !createdAt.isBefore(filter.getCreatedTo()))) {
            return false;
        }
        return true;
    }
}
