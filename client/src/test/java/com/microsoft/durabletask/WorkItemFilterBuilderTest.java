// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for work item filter integration in {@link DurableTaskGrpcWorkerBuilder}.
 */
public class WorkItemFilterBuilderTest {

    @Test
    void useWorkItemFilters_explicit_setsFilter() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("Orch1")
                .addActivity("Act1")
                .build();

        // Build method is private on the worker, but we can test that builder accepts the filter
        // and constructs a worker without error.
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addOrchestration(new TestOrchestrationFactory("Orch1"));
        builder.addActivity(new TestActivityFactory("Act1"));
        builder.useWorkItemFilters(filter);

        // Should not throw
        DurableTaskGrpcWorker worker = builder.build();
        assertNotNull(worker);
        worker.close();
    }

    @Test
    void useWorkItemFilters_auto_generatesFromRegistered() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addOrchestration(new TestOrchestrationFactory("Orch1"));
        builder.addOrchestration(new TestOrchestrationFactory("Orch2"));
        builder.addActivity(new TestActivityFactory("Act1"));
        builder.useWorkItemFilters();

        DurableTaskGrpcWorker worker = builder.build();
        assertNotNull(worker);

        WorkItemFilter filter = worker.getWorkItemFilter();
        assertNotNull(filter);
        assertEquals(2, filter.getOrchestrations().size());
        assertEquals(1, filter.getActivities().size());

        Set<String> orchNames = filter.getOrchestrations().stream()
                .map(WorkItemFilter.OrchestrationFilter::getName)
                .collect(Collectors.toSet());
        assertTrue(orchNames.contains("Orch1"));
        assertTrue(orchNames.contains("Orch2"));

        assertEquals("Act1", filter.getActivities().get(0).getName());
        worker.close();
    }

    @Test
    void useWorkItemFilters_auto_withStrictVersioning_includesVersion() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addOrchestration(new TestOrchestrationFactory("Orch1"));
        builder.addActivity(new TestActivityFactory("Act1"));
        builder.useVersioning(new DurableTaskGrpcWorkerVersioningOptions(
                "1.0", "1.0",
                DurableTaskGrpcWorkerVersioningOptions.VersionMatchStrategy.STRICT,
                DurableTaskGrpcWorkerVersioningOptions.VersionFailureStrategy.REJECT));
        builder.useWorkItemFilters();

        DurableTaskGrpcWorker worker = builder.build();
        assertNotNull(worker);

        WorkItemFilter filter = worker.getWorkItemFilter();
        assertNotNull(filter);
        assertEquals(1, filter.getOrchestrations().size());
        assertEquals(1, filter.getOrchestrations().get(0).getVersions().size());
        assertEquals("1.0", filter.getOrchestrations().get(0).getVersions().get(0));

        assertEquals(1, filter.getActivities().size());
        assertEquals(1, filter.getActivities().get(0).getVersions().size());
        assertEquals("1.0", filter.getActivities().get(0).getVersions().get(0));
        worker.close();
    }

    @Test
    void useWorkItemFilters_auto_withNoneVersioning_noVersions() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addOrchestration(new TestOrchestrationFactory("Orch1"));
        builder.useVersioning(new DurableTaskGrpcWorkerVersioningOptions(
                "1.0", "1.0",
                DurableTaskGrpcWorkerVersioningOptions.VersionMatchStrategy.NONE,
                DurableTaskGrpcWorkerVersioningOptions.VersionFailureStrategy.REJECT));
        builder.useWorkItemFilters();

        DurableTaskGrpcWorker worker = builder.build();
        assertNotNull(worker);

        WorkItemFilter filter = worker.getWorkItemFilter();
        assertNotNull(filter);
        assertTrue(filter.getOrchestrations().get(0).getVersions().isEmpty());
        worker.close();
    }

    @Test
    void useWorkItemFilters_nullExplicit_disablesFiltering() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addOrchestration(new TestOrchestrationFactory("Orch1"));
        builder.useWorkItemFilters(null);

        DurableTaskGrpcWorker worker = builder.build();
        assertNotNull(worker);

        assertNull(worker.getWorkItemFilter());
        worker.close();
    }

    @Test
    void noWorkItemFilters_filterIsNull() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addOrchestration(new TestOrchestrationFactory("Orch1"));

        DurableTaskGrpcWorker worker = builder.build();
        assertNotNull(worker);

        assertNull(worker.getWorkItemFilter());
        worker.close();
    }

    // Simple test factory for orchestrations
    private static class TestOrchestrationFactory implements TaskOrchestrationFactory {
        private final String name;

        TestOrchestrationFactory(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public TaskOrchestration create() {
            return ctx -> { };
        }
    }

    // Simple test factory for activities
    private static class TestActivityFactory implements TaskActivityFactory {
        private final String name;

        TestActivityFactory(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public TaskActivity create() {
            return ctx -> null;
        }
    }
}
