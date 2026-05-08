// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.DurableEntityClient;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.EntityMetadata;
import com.microsoft.durabletask.EntityQuery;
import com.microsoft.durabletask.EntityQueryResult;
import com.microsoft.durabletask.exporthistory.models.ExportJobDescription;
import com.microsoft.durabletask.exporthistory.models.ExportJobQuery;
import com.microsoft.durabletask.exporthistory.models.ExportJobState;
import com.microsoft.durabletask.exporthistory.models.ExportJobStatus;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DefaultExportHistoryClient} focusing on query filtering behavior.
 */
@ExtendWith(MockitoExtension.class)
class DefaultExportHistoryClientTest {

    @Mock
    private DurableTaskClient durableTaskClient;

    @Mock
    private DurableEntityClient durableEntityClient;

    private DefaultExportHistoryClient client;

    @BeforeEach
    void setUp() {
        ExportHistoryStorageOptions storageOptions = ExportHistoryStorageOptions.newBuilder()
                .connectionString("DefaultEndpointsProtocol=https;AccountName=test")
                .containerName("test-container")
                .build();
        lenient().when(durableTaskClient.getEntities()).thenReturn(durableEntityClient);
        client = new DefaultExportHistoryClient(durableTaskClient, storageOptions);
    }

    @Test
    void listJobs_createdFromFilter_excludesJobsBeforeThreshold() {
        Instant threshold = Instant.parse("2026-03-01T00:00:00Z");

        ExportJobState oldJob = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-02-15T00:00:00Z"));
        ExportJobState newJob = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-03-15T00:00:00Z"));

        setupEntityQueryResult(
                createMockMetadata("old-job", oldJob),
                createMockMetadata("new-job", newJob));

        ExportJobQuery query = new ExportJobQuery();
        query.setCreatedFrom(threshold);

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(1, results.size());
        assertEquals("new-job", results.get(0).getJobId());
    }

    @Test
    void listJobs_createdToFilter_excludesJobsAfterThreshold() {
        Instant threshold = Instant.parse("2026-03-01T00:00:00Z");

        ExportJobState earlyJob = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-02-15T00:00:00Z"));
        ExportJobState lateJob = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-04-01T00:00:00Z"));

        setupEntityQueryResult(
                createMockMetadata("early-job", earlyJob),
                createMockMetadata("late-job", lateJob));

        ExportJobQuery query = new ExportJobQuery();
        query.setCreatedTo(threshold);

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(1, results.size());
        assertEquals("early-job", results.get(0).getJobId());
    }

    @Test
    void listJobs_createdFromAndToFilter_returnsOnlyJobsInWindow() {
        Instant from = Instant.parse("2026-02-01T00:00:00Z");
        Instant to = Instant.parse("2026-04-01T00:00:00Z");

        ExportJobState tooEarly = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-01-15T00:00:00Z"));
        ExportJobState inRange = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-03-01T00:00:00Z"));
        ExportJobState tooLate = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-05-01T00:00:00Z"));

        setupEntityQueryResult(
                createMockMetadata("too-early", tooEarly),
                createMockMetadata("in-range", inRange),
                createMockMetadata("too-late", tooLate));

        ExportJobQuery query = new ExportJobQuery();
        query.setCreatedFrom(from);
        query.setCreatedTo(to);

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(1, results.size());
        assertEquals("in-range", results.get(0).getJobId());
    }

    @Test
    void listJobs_nullCreatedAt_excludedWhenCreatedFromSet() {
        ExportJobState noTimestamp = createState(ExportJobStatus.ACTIVE, null);
        ExportJobState withTimestamp = createState(ExportJobStatus.ACTIVE,
                Instant.parse("2026-03-15T00:00:00Z"));

        setupEntityQueryResult(
                createMockMetadata("no-ts", noTimestamp),
                createMockMetadata("with-ts", withTimestamp));

        ExportJobQuery query = new ExportJobQuery();
        query.setCreatedFrom(Instant.parse("2026-03-01T00:00:00Z"));

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(1, results.size());
        assertEquals("with-ts", results.get(0).getJobId());
    }

    @Test
    void listJobs_nullCreatedAt_excludedWhenCreatedToSet() {
        ExportJobState noTimestamp = createState(ExportJobStatus.ACTIVE, null);

        setupEntityQueryResult(createMockMetadata("no-ts", noTimestamp));

        ExportJobQuery query = new ExportJobQuery();
        query.setCreatedTo(Instant.parse("2026-12-31T00:00:00Z"));

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(0, results.size());
    }

    @Test
    void listJobs_statusAndTimeFilters_combinedCorrectly() {
        ExportJobState completedInRange = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-03-01T00:00:00Z"));
        ExportJobState activeInRange = createState(ExportJobStatus.ACTIVE,
                Instant.parse("2026-03-01T00:00:00Z"));
        ExportJobState completedOutOfRange = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-01-01T00:00:00Z"));

        setupEntityQueryResult(
                createMockMetadata("c-in", completedInRange),
                createMockMetadata("a-in", activeInRange),
                createMockMetadata("c-out", completedOutOfRange));

        ExportJobQuery query = new ExportJobQuery();
        query.setStatus(ExportJobStatus.COMPLETED);
        query.setCreatedFrom(Instant.parse("2026-02-01T00:00:00Z"));

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(1, results.size());
        assertEquals("c-in", results.get(0).getJobId());
    }

    @Test
    void listJobs_noTimeFilters_returnsAll() {
        ExportJobState job1 = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-01-01T00:00:00Z"));
        ExportJobState job2 = createState(ExportJobStatus.ACTIVE,
                Instant.parse("2026-06-01T00:00:00Z"));

        setupEntityQueryResult(
                createMockMetadata("job1", job1),
                createMockMetadata("job2", job2));

        List<ExportJobDescription> results = collectResults(client.listJobs(null));

        assertEquals(2, results.size());
    }

    @Test
    void listJobs_filteredPageEmptyButContinuationTokenSet_followsToNextPage() {
        // Page 1: two ACTIVE jobs that filter to empty for COMPLETED, with continuation token.
        ExportJobState active1 = createState(ExportJobStatus.ACTIVE,
                Instant.parse("2026-03-01T00:00:00Z"));
        ExportJobState active2 = createState(ExportJobStatus.ACTIVE,
                Instant.parse("2026-03-02T00:00:00Z"));
        EntityMetadata activeMeta1 = createMockMetadata("active-1", active1);
        EntityMetadata activeMeta2 = createMockMetadata("active-2", active2);
        List<EntityMetadata> page1Entities = Arrays.asList(activeMeta1, activeMeta2);
        EntityQueryResult page1 = mock(EntityQueryResult.class);
        when(page1.getEntities()).thenReturn(page1Entities);
        when(page1.getContinuationToken()).thenReturn("token-page-2");

        // Page 2: two COMPLETED jobs that match the filter, no further pages.
        ExportJobState completed1 = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-03-03T00:00:00Z"));
        ExportJobState completed2 = createState(ExportJobStatus.COMPLETED,
                Instant.parse("2026-03-04T00:00:00Z"));
        EntityMetadata completedMeta1 = createMockMetadata("completed-1", completed1);
        EntityMetadata completedMeta2 = createMockMetadata("completed-2", completed2);
        List<EntityMetadata> page2Entities = Arrays.asList(completedMeta1, completedMeta2);
        EntityQueryResult page2 = mock(EntityQueryResult.class);
        when(page2.getEntities()).thenReturn(page2Entities);
        when(page2.getContinuationToken()).thenReturn(null);

        when(durableEntityClient.queryEntities(any(EntityQuery.class)))
                .thenReturn(page1, page2);

        ExportJobQuery query = new ExportJobQuery();
        query.setStatus(ExportJobStatus.COMPLETED);
        query.setPageSize(2);

        List<ExportJobDescription> results = collectResults(client.listJobs(query));

        assertEquals(2, results.size());
        assertEquals("completed-1", results.get(0).getJobId());
        assertEquals("completed-2", results.get(1).getJobId());
    }

    // region Helpers

    private ExportJobState createState(ExportJobStatus status, Instant createdAt) {
        ExportJobState state = new ExportJobState();
        state.setStatus(status);
        state.setCreatedAt(createdAt);
        return state;
    }

    private EntityMetadata createMockMetadata(String key, ExportJobState state) {
        EntityMetadata metadata = mock(EntityMetadata.class);
        lenient().when(metadata.readStateAs(ExportJobState.class)).thenReturn(state);
        lenient().when(metadata.getEntityInstanceId()).thenReturn(new EntityInstanceId("ExportJob", key));
        return metadata;
    }

    private void setupEntityQueryResult(EntityMetadata... metadataArray) {
        EntityQueryResult result = mock(EntityQueryResult.class);
        when(result.getEntities()).thenReturn(Arrays.asList(metadataArray));
        when(result.getContinuationToken()).thenReturn(null);
        when(durableEntityClient.queryEntities(any(EntityQuery.class))).thenReturn(result);
    }

    private List<ExportJobDescription> collectResults(ExportJobQueryPageable pageable) {
        List<ExportJobDescription> results = new ArrayList<>();
        for (ExportJobDescription desc : pageable) {
            results.add(desc);
        }
        return results;
    }

    // endregion
}
