// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.JacksonDataConverter;
import com.microsoft.durabletask.TaskEntityContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for the {@link ExportJob} entity's {@code create} operation.
 */
class ExportJobTest {

    @Test
    void create_delayedContinuousJob_preservesTheSubmittedLowerBound() throws Exception {
        ExportJob entity = newEntityWithPendingState();
        Instant submittedAt = Instant.parse("2026-07-01T10:00:00Z");
        Instant processedAt = submittedAt.plusSeconds(300);
        JacksonDataConverter converter = new JacksonDataConverter();
        try (MockedStatic<Instant> clock = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
            clock.when(Instant::now).thenReturn(submittedAt);
            ExportJobCreationOptions options = new ExportJobCreationOptions("job-continuous")
                    .setMode(ExportMode.CONTINUOUS)
                    .setDestination(new ExportDestination("container"));
            String request = converter.serialize(options.copy());

            clock.when(Instant::now).thenReturn(processedAt);
            entity.create(converter.deserialize(request, ExportJobCreationOptions.class));

            assertEquals(submittedAt, entity.get().getConfig().getFilter().getCompletedTimeFrom());
            assertEquals(processedAt, entity.get().getCreatedAt());
        }
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
