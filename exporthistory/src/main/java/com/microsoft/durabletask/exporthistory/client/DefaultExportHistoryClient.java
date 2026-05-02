// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.EntityMetadata;
import com.microsoft.durabletask.EntityQuery;
import com.microsoft.durabletask.EntityQueryResult;
import com.microsoft.durabletask.exporthistory.entity.ExportJob;
import com.microsoft.durabletask.exporthistory.exception.ExportJobNotFoundException;
import com.microsoft.durabletask.exporthistory.models.*;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of {@link ExportHistoryClient} using durable entities.
 */
public class DefaultExportHistoryClient extends ExportHistoryClient {

    private static final Logger logger = Logger.getLogger(DefaultExportHistoryClient.class.getName());

    private final DurableTaskClient durableTaskClient;
    private final ExportHistoryStorageOptions storageOptions;

    public DefaultExportHistoryClient(
            @Nonnull DurableTaskClient durableTaskClient,
            @Nonnull ExportHistoryStorageOptions storageOptions) {
        if (durableTaskClient == null) {
            throw new IllegalArgumentException("durableTaskClient must not be null.");
        }
        if (storageOptions == null) {
            throw new IllegalArgumentException("storageOptions must not be null.");
        }
        this.durableTaskClient = durableTaskClient;
        this.storageOptions = storageOptions;
    }

    @Override
    @Nonnull
    public ExportHistoryJobClient createJob(@Nonnull ExportJobCreationOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }
        logger.info("Creating export job: " + options.getJobId());
        ExportHistoryJobClient jobClient = this.getJobClient(options.getJobId());
        jobClient.create(options);
        return jobClient;
    }

    @Override
    @Nonnull
    public ExportJobDescription getJob(@Nonnull String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            throw new IllegalArgumentException("jobId must not be null or empty.");
        }
        return this.getJobClient(jobId).describe();
    }

    @Override
    @Nonnull
    public ExportJobQueryPageable listJobs(@Nullable ExportJobQuery filter) {
        return new ExportJobQueryPageable(filter, query -> {
            EntityQuery entityQuery = new EntityQuery();
            entityQuery.setInstanceIdStartsWith(
                    "@" + ExportJob.class.getSimpleName() + "@" +
                    (query != null && query.getJobIdPrefix() != null ? query.getJobIdPrefix() : ""));
            entityQuery.setIncludeState(true);
            entityQuery.setPageSize(query != null && query.getPageSize() != null
                    ? query.getPageSize()
                    : ExportJobQuery.DEFAULT_PAGE_SIZE);
            if (query != null && query.getContinuationToken() != null) {
                entityQuery.setContinuationToken(query.getContinuationToken());
            }

            EntityQueryResult entityResult = this.durableTaskClient.getEntities().queryEntities(entityQuery);
            List<ExportJobDescription> exportJobs = new ArrayList<>();
            for (EntityMetadata metadata : entityResult.getEntities()) {
                ExportJobState state = metadata.readStateAs(ExportJobState.class);
                if (state == null) {
                    continue;
                }
                if (query != null && query.getStatus() != null && state.getStatus() != query.getStatus()) {
                    continue;
                }
                ExportJobDescription desc = new ExportJobDescription();
                desc.setJobId(metadata.getEntityInstanceId().getKey());
                desc.setStatus(state.getStatus());
                desc.setCreatedAt(state.getCreatedAt());
                desc.setLastModifiedAt(state.getLastModifiedAt());
                desc.setConfig(state.getConfig());
                desc.setOrchestratorInstanceId(state.getOrchestratorInstanceId());
                desc.setScannedInstances(state.getScannedInstances());
                desc.setExportedInstances(state.getExportedInstances());
                desc.setLastError(state.getLastError());
                desc.setCheckpoint(state.getCheckpoint());
                desc.setLastCheckpointTime(state.getLastCheckpointTime());
                exportJobs.add(desc);
            }

            return new ExportJobQueryPageable.ExportJobQueryResult(
                    exportJobs,
                    entityResult.getContinuationToken());
        });
    }

    @Override
    @Nonnull
    public ExportHistoryJobClient getJobClient(@Nonnull String jobId) {
        return new DefaultExportHistoryJobClient(
                this.durableTaskClient, jobId, this.storageOptions);
    }
}
