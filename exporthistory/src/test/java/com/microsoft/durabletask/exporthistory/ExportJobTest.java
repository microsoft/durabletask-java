// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link ExportJob} entity's {@code create} operation.s
 */
class ExportJobTest {

    @Test
    void create_continuousWithoutCompletedTimeFrom_defaultsToCreationInstant() throws Exception {
        ExportJob entity = newEntityWithPendingState();

        Instant before = Instant.now();
        entity.create(new ExportJobCreationOptions("job-continuous")
                .setMode(ExportMode.CONTINUOUS)
                .setDestination(new ExportDestination("container")));
        Instant after = Instant.now();

        Instant completedTimeFrom = entity.get().getConfig().getFilter().getCompletedTimeFrom();
        assertNotNull(completedTimeFrom,
                "CONTINUOUS create must default completedTimeFrom so it does not re-export the whole task hub");
        assertFalse(completedTimeFrom.isBefore(before), "defaulted completedTimeFrom must be at/after job creation");
        assertFalse(completedTimeFrom.isAfter(after), "defaulted completedTimeFrom must be at/before job creation");

        assertEquals(entity.get().getCreatedAt(), completedTimeFrom);
    }

    @Test
    void create_explicitCompletedTimeFrom_isPreserved() throws Exception {
        ExportJob entity = newEntityWithPendingState();
        Instant explicit = Instant.parse("2026-06-01T00:00:00Z");

        entity.create(new ExportJobCreationOptions("job-explicit")
                .setMode(ExportMode.CONTINUOUS)
                .setCompletedTimeFrom(explicit)
                .setDestination(new ExportDestination("container")));

        assertEquals(explicit, entity.get().getConfig().getFilter().getCompletedTimeFrom());
    }

    private static ExportJob newEntityWithPendingState() throws Exception {
        ExportJob entity = new ExportJob();
        ExportJobState state = new ExportJobState();
        state.setStatus(ExportJobStatus.PENDING);
        setBaseField(entity, "state", state);
        setBaseField(entity, "context", mock(TaskEntityContext.class));
        return entity;
    }

    private static void setBaseField(Object target, String name, Object value) throws Exception {
        Field field = AbstractTaskEntity.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
