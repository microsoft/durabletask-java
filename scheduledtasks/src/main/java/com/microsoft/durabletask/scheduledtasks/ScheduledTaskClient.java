// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityMetadata;
import com.microsoft.durabletask.EntityQuery;
import com.microsoft.durabletask.EntityQueryResult;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Client for managing scheduled tasks. Obtain an instance via
 * {@link ScheduledTaskClientExtensions#useScheduledTasks(DurableTaskClient)}.
 */
public final class ScheduledTaskClient {

    private static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(60);
    private static final String ENTITY_ID_PREFIX = "@" + Schedule.NAME.toLowerCase(Locale.ROOT) + "@";

    private final DurableTaskClient client;
    private final Duration operationTimeout;

    ScheduledTaskClient(DurableTaskClient client) {
        this(client, DEFAULT_OPERATION_TIMEOUT);
    }

    ScheduledTaskClient(DurableTaskClient client, Duration operationTimeout) {
        this.client = client;
        this.operationTimeout = operationTimeout;
    }

    /**
     * Gets a client for managing a specific schedule without creating it.
     *
     * @param scheduleId the schedule ID
     * @return a schedule-bound client
     */
    public ScheduleClient getScheduleClient(String scheduleId) {
        return new ScheduleClient(this.client, scheduleId, this.operationTimeout);
    }

    /**
     * Creates a new schedule (or replaces an existing one) and returns a client bound to it.
     *
     * @param options the creation options
     * @return a schedule-bound client
     */
    public ScheduleClient createSchedule(ScheduleCreationOptions options) {
        if (options == null) {
            throw new ScheduleClientValidationException("", "options must not be null.");
        }
        ScheduleClient scheduleClient = getScheduleClient(options.getScheduleId());
        scheduleClient.create(options);
        return scheduleClient;
    }

    /**
     * Gets a schedule description by ID.
     *
     * @param scheduleId the schedule ID
     * @return the schedule description, or {@code null} if the schedule does not exist
     */
    @Nullable
    public ScheduleDescription getSchedule(String scheduleId) {
        try {
            return getScheduleClient(scheduleId).describe();
        } catch (ScheduleNotFoundException e) {
            return null;
        }
    }

    /**
     * Lists schedules matching the query, auto-paginating on demand.
     * <p>
     * The schedule-ID prefix is applied by the backend; status and creation-time filters are applied client-side to
     * each page, so a page may be underfilled or empty while later pages still hold matches.
     *
     * @param query the query, or {@code null} for defaults
     * @return an auto-paginating iterable of schedule descriptions
     */
    public ScheduleQueryPageable listSchedules(@Nullable ScheduleQuery query) {
        final ScheduleQuery effective = query == null ? new ScheduleQuery() : query;
        final String prefix = effective.getScheduleIdPrefix() == null ? "" : effective.getScheduleIdPrefix();
        final int pageSize = effective.getPageSize() == null
                ? ScheduleQuery.DEFAULT_PAGE_SIZE
                : effective.getPageSize();

        return new ScheduleQueryPageable(
                effective.getContinuationToken(),
                continuationToken -> fetchPage(effective, prefix, pageSize, continuationToken));
    }

    private ScheduleQueryResult fetchPage(
            ScheduleQuery query, String prefix, int pageSize, @Nullable String continuationToken) {
        EntityQuery entityQuery = new EntityQuery()
                .setInstanceIdStartsWith(ENTITY_ID_PREFIX + prefix)
                .setIncludeState(true)
                .setPageSize(pageSize)
                .setContinuationToken(continuationToken);

        EntityQueryResult result = this.client.getEntities().queryEntities(entityQuery);

        List<ScheduleDescription> descriptions = new ArrayList<>();
        for (EntityMetadata metadata : result.getEntities()) {
            ScheduleState state = metadata.readStateAs(ScheduleState.class);
            // Skip entities without a configuration. This defensively avoids mapping a transient uninitialized
            // state (e.g. left by a stale post-delete signal); the .NET SDK would throw on such an entity.
            if (state == null || state.getScheduleConfiguration() == null) {
                continue;
            }
            if (!matchesFilter(state, query)) {
                continue;
            }
            descriptions.add(ScheduleDescription.fromState(metadata.getEntityInstanceId().getKey(), state));
        }
        return new ScheduleQueryResult(descriptions, result.getContinuationToken());
    }

    private static boolean matchesFilter(ScheduleState state, ScheduleQuery query) {
        if (query.getStatus() != null && state.getStatus() != query.getStatus()) {
            return false;
        }
        OffsetDateTime createdAt = state.getScheduleCreatedAt();
        // Bounds are exclusive on both ends, matching the .NET SDK.
        if (query.getCreatedFrom() != null) {
            if (createdAt == null || !afterInstant(createdAt, query.getCreatedFrom())) {
                return false;
            }
        }
        if (query.getCreatedTo() != null) {
            if (createdAt == null || !beforeInstant(createdAt, query.getCreatedTo())) {
                return false;
            }
        }
        return true;
    }

    private static boolean afterInstant(OffsetDateTime value, OffsetDateTime bound) {
        Instant v = value.toInstant();
        Instant b = bound.toInstant();
        return v.isAfter(b);
    }

    private static boolean beforeInstant(OffsetDateTime value, OffsetDateTime bound) {
        Instant v = value.toInstant();
        Instant b = bound.toInstant();
        return v.isBefore(b);
    }
}
