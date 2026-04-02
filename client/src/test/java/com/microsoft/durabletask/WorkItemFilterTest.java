// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.WorkItemFilters;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link WorkItemFilter} and its builder.
 */
public class WorkItemFilterTest {

    @Test
    void newBuilder_emptyFilter_hasEmptyLists() {
        WorkItemFilter filter = WorkItemFilter.newBuilder().build();
        assertNotNull(filter.getOrchestrations());
        assertNotNull(filter.getActivities());
        assertTrue(filter.getOrchestrations().isEmpty());
        assertTrue(filter.getActivities().isEmpty());
    }

    @Test
    void addOrchestration_singleName_isRetained() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("MyOrchestrator")
                .build();

        assertEquals(1, filter.getOrchestrations().size());
        assertEquals("MyOrchestrator", filter.getOrchestrations().get(0).getName());
        assertTrue(filter.getOrchestrations().get(0).getVersions().isEmpty());
    }

    @Test
    void addOrchestration_withVersions_retainsVersions() {
        List<String> versions = Arrays.asList("1.0", "2.0");
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("MyOrchestrator", versions)
                .build();

        assertEquals(1, filter.getOrchestrations().size());
        WorkItemFilter.OrchestrationFilter orch = filter.getOrchestrations().get(0);
        assertEquals("MyOrchestrator", orch.getName());
        assertEquals(2, orch.getVersions().size());
        assertEquals("1.0", orch.getVersions().get(0));
        assertEquals("2.0", orch.getVersions().get(1));
    }

    @Test
    void addOrchestration_nullVersions_treatedAsEmpty() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("MyOrchestrator", null)
                .build();

        assertEquals(1, filter.getOrchestrations().size());
        assertTrue(filter.getOrchestrations().get(0).getVersions().isEmpty());
    }

    @Test
    void addActivity_singleName_isRetained() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addActivity("MyActivity")
                .build();

        assertEquals(1, filter.getActivities().size());
        assertEquals("MyActivity", filter.getActivities().get(0).getName());
        assertTrue(filter.getActivities().get(0).getVersions().isEmpty());
    }

    @Test
    void addActivity_withVersions_retainsVersions() {
        List<String> versions = Collections.singletonList("v1");
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addActivity("MyActivity", versions)
                .build();

        assertEquals(1, filter.getActivities().size());
        WorkItemFilter.ActivityFilter act = filter.getActivities().get(0);
        assertEquals("MyActivity", act.getName());
        assertEquals(1, act.getVersions().size());
        assertEquals("v1", act.getVersions().get(0));
    }

    @Test
    void addActivity_nullVersions_treatedAsEmpty() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addActivity("MyActivity", null)
                .build();

        assertEquals(1, filter.getActivities().size());
        assertTrue(filter.getActivities().get(0).getVersions().isEmpty());
    }

    @Test
    void addOrchestration_nullName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WorkItemFilter.newBuilder().addOrchestration(null));
    }

    @Test
    void addOrchestration_emptyName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WorkItemFilter.newBuilder().addOrchestration(""));
    }

    @Test
    void addActivity_nullName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WorkItemFilter.newBuilder().addActivity(null));
    }

    @Test
    void addActivity_emptyName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WorkItemFilter.newBuilder().addActivity(""));
    }

    @Test
    void addOrchestrationWithVersions_nullName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WorkItemFilter.newBuilder().addOrchestration(null, Collections.singletonList("v1")));
    }

    @Test
    void addActivityWithVersions_nullName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WorkItemFilter.newBuilder().addActivity(null, Collections.singletonList("v1")));
    }

    @Test
    void multipleFilters_allRetained() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("Orch1")
                .addOrchestration("Orch2")
                .addActivity("Act1")
                .addActivity("Act2")
                .addActivity("Act3")
                .build();

        assertEquals(2, filter.getOrchestrations().size());
        assertEquals(3, filter.getActivities().size());
        assertEquals("Orch1", filter.getOrchestrations().get(0).getName());
        assertEquals("Orch2", filter.getOrchestrations().get(1).getName());
        assertEquals("Act1", filter.getActivities().get(0).getName());
        assertEquals("Act2", filter.getActivities().get(1).getName());
        assertEquals("Act3", filter.getActivities().get(2).getName());
    }

    @Test
    void orchestrationsList_isUnmodifiable() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("Orch1")
                .build();

        assertThrows(UnsupportedOperationException.class, () ->
                filter.getOrchestrations().add(new WorkItemFilter.OrchestrationFilter("x", Collections.<String>emptyList())));
    }

    @Test
    void activitiesList_isUnmodifiable() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addActivity("Act1")
                .build();

        assertThrows(UnsupportedOperationException.class, () ->
                filter.getActivities().add(new WorkItemFilter.ActivityFilter("x", Collections.<String>emptyList())));
    }

    @Test
    void toProtoWorkItemFilters_convertsOrchestrationsAndActivities() {
        WorkItemFilter filter = WorkItemFilter.newBuilder()
                .addOrchestration("Orch1", Arrays.asList("1.0", "2.0"))
                .addOrchestration("Orch2")
                .addActivity("Act1", Collections.singletonList("v1"))
                .addActivity("Act2")
                .build();

        WorkItemFilters proto = DurableTaskGrpcWorker.toProtoWorkItemFilters(filter);

        assertEquals(2, proto.getOrchestrationsCount());
        assertEquals("Orch1", proto.getOrchestrations(0).getName());
        assertEquals(Arrays.asList("1.0", "2.0"), proto.getOrchestrations(0).getVersionsList());
        assertEquals("Orch2", proto.getOrchestrations(1).getName());
        assertTrue(proto.getOrchestrations(1).getVersionsList().isEmpty());

        assertEquals(2, proto.getActivitiesCount());
        assertEquals("Act1", proto.getActivities(0).getName());
        assertEquals(Collections.singletonList("v1"), proto.getActivities(0).getVersionsList());
        assertEquals("Act2", proto.getActivities(1).getName());
        assertTrue(proto.getActivities(1).getVersionsList().isEmpty());
    }

    @Test
    void toProtoWorkItemFilters_emptyFilter_producesEmptyProto() {
        WorkItemFilter filter = WorkItemFilter.newBuilder().build();

        WorkItemFilters proto = DurableTaskGrpcWorker.toProtoWorkItemFilters(filter);

        assertEquals(0, proto.getOrchestrationsCount());
        assertEquals(0, proto.getActivitiesCount());
    }
}
