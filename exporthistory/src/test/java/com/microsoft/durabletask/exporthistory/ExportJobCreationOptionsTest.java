// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.JacksonDataConverter;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for {@link ExportJobCreationOptions}.
 */
class ExportJobCreationOptionsTest {

    @Test
    void defaults_areCorrect() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1");
        assertEquals("job-1", options.getJobId());
        assertEquals(ExportMode.BATCH, options.getMode());
        assertNull(options.getCompletedTimeFrom());
        assertEquals(100, options.getMaxInstancesPerBatch());
        assertEquals(ExportFormatKind.JSONL, options.getFormat().getKind());
        assertEquals(
                Arrays.asList(
                        OrchestrationRuntimeStatus.COMPLETED,
                        OrchestrationRuntimeStatus.FAILED,
                        OrchestrationRuntimeStatus.TERMINATED),
                options.getRuntimeStatus());
    }

    @Test
    void continuousDefault_isCapturedAtConstructionAndPreservedByCopyAndSerialization() {
        Instant constructedAt = Instant.parse("2026-07-01T10:00:00Z");
        Instant queuedAt = constructedAt.plusSeconds(300);
        JacksonDataConverter converter = new JacksonDataConverter();
        try (MockedStatic<Instant> clock = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
            clock.when(Instant::now).thenReturn(constructedAt);
            ExportJobCreationOptions options = new ExportJobCreationOptions("job-1");

            clock.when(Instant::now).thenReturn(queuedAt);
            options.setMode(ExportMode.CONTINUOUS);
            assertEquals(constructedAt, options.getCompletedTimeFrom());
            ExportJobCreationOptions copy = options.copy();
            assertEquals(constructedAt, copy.getCompletedTimeFrom());

            String serialized = converter.serialize(copy);
            assertFalse(serialized.contains("optionsCreatedAt"));
            ExportJobCreationOptions restored = converter.deserialize(serialized, ExportJobCreationOptions.class);
            assertEquals(ExportMode.CONTINUOUS, restored.getMode());
            assertEquals(constructedAt, restored.getCompletedTimeFrom());
            assertEquals(constructedAt, restored.copy().getCompletedTimeFrom());
        }
    }

    @Test
    void continuousDefault_doesNotSupplyTheRequiredBatchLowerBound() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.CONTINUOUS);
        assertNotNull(options.getCompletedTimeFrom());
        options.setMode(ExportMode.BATCH).setCompletedTimeTo(Instant.now().minusSeconds(60));
        assertNull(options.getCompletedTimeFrom());
        assertThrows(IllegalArgumentException.class, options::validateForCreate);
    }

    @Test
    void continuousExplicitLowerBound_canBeClearedToTheConstructionDefault() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.CONTINUOUS);
        Instant defaultFrom = options.getCompletedTimeFrom();
        Instant explicit = Instant.parse("2026-06-01T00:00:00Z");
        options.setCompletedTimeFrom(explicit);
        assertEquals(explicit, options.getCompletedTimeFrom());
        assertEquals(explicit, options.copy().getCompletedTimeFrom());
        options.setCompletedTimeFrom(null);
        assertEquals(defaultFrom, options.getCompletedTimeFrom());
    }

    @Test
    void continuousExplicitLowerBound_beforeModeSelection_survivesSerialization() {
        Instant explicit = Instant.parse("2026-06-01T00:00:00Z");
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setCompletedTimeFrom(explicit)
                .setMode(ExportMode.CONTINUOUS);
        JacksonDataConverter converter = new JacksonDataConverter();
        ExportJobCreationOptions restored = converter.deserialize(
                converter.serialize(options.copy()), ExportJobCreationOptions.class);
        assertEquals(explicit, options.getCompletedTimeFrom());
        assertEquals(explicit, restored.getCompletedTimeFrom());
    }

    @Test
    void nullOrEmptyJobId_generatesId() {
        assertNotNull(new ExportJobCreationOptions(null).getJobId());
        assertFalse(new ExportJobCreationOptions(null).getJobId().isEmpty());
        assertFalse(new ExportJobCreationOptions("").getJobId().isEmpty());
    }

    @Test
    void fluentSetters_returnSameInstance() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1");
        assertSame(options, options.setMode(ExportMode.CONTINUOUS));
        assertSame(options, options.setCompletedTimeFrom(Instant.now()));
        assertSame(options, options.setCompletedTimeTo(null));
        assertSame(options, options.setMaxInstancesPerBatch(50));
        assertSame(options, options.setRuntimeStatus(null));
    }

    @Test
    void setMaxInstancesPerBatch_outOfRange_throws() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1");
        assertThrows(IllegalArgumentException.class, () -> options.setMaxInstancesPerBatch(0));
        assertThrows(IllegalArgumentException.class, () -> options.setMaxInstancesPerBatch(1001));
    }

    @Test
    void setMaxInstancesPerBatch_atBounds_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1");
        assertEquals(1, options.setMaxInstancesPerBatch(1).getMaxInstancesPerBatch());
        assertEquals(1000, options.setMaxInstancesPerBatch(1000).getMaxInstancesPerBatch());
    }

    @Test
    void setRuntimeStatus_nonTerminal_throws() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1");
        assertThrows(IllegalArgumentException.class,
                () -> options.setRuntimeStatus(Collections.singletonList(OrchestrationRuntimeStatus.RUNNING)));
    }

    @Test
    void setRuntimeStatus_terminalSubset_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setRuntimeStatus(Collections.singletonList(OrchestrationRuntimeStatus.COMPLETED));
        assertEquals(Collections.singletonList(OrchestrationRuntimeStatus.COMPLETED), options.getRuntimeStatus());
    }

    @Test
    void setRuntimeStatus_nullOrEmpty_resetsToAllTerminal() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setRuntimeStatus(Collections.singletonList(OrchestrationRuntimeStatus.COMPLETED))
                .setRuntimeStatus(null);
        assertEquals(3, options.getRuntimeStatus().size());
        assertTrue(options.getRuntimeStatus().contains(OrchestrationRuntimeStatus.TERMINATED));
    }

    @Test
    void validateForCreate_batchRequiresWindow() {
        ExportJobCreationOptions noFrom = new ExportJobCreationOptions("job-1").setMode(ExportMode.BATCH);
        assertThrows(IllegalArgumentException.class, noFrom::validateForCreate);

        ExportJobCreationOptions noTo = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.BATCH)
                .setCompletedTimeFrom(Instant.now().minusSeconds(3600));
        assertThrows(IllegalArgumentException.class, noTo::validateForCreate);
    }

    @Test
    void validateForCreate_batchToBeforeFrom_throws() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.BATCH)
                .setCompletedTimeFrom(Instant.now().minusSeconds(60))
                .setCompletedTimeTo(Instant.now().minusSeconds(120));
        assertThrows(IllegalArgumentException.class, options::validateForCreate);
    }

    @Test
    void validateForCreate_batchToInFuture_throws() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.BATCH)
                .setCompletedTimeFrom(Instant.now().minusSeconds(60))
                .setCompletedTimeTo(Instant.now().plusSeconds(3600));
        assertThrows(IllegalArgumentException.class, options::validateForCreate);
    }

    @Test
    void validateForCreate_validBatch_passes() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.BATCH)
                .setCompletedTimeFrom(Instant.now().minusSeconds(3600))
                .setCompletedTimeTo(Instant.now().minusSeconds(60));
        options.validateForCreate();
    }

    @Test
    void validateForCreate_continuousWithTo_throws() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.CONTINUOUS)
                .setCompletedTimeTo(Instant.now().minusSeconds(60));
        assertThrows(IllegalArgumentException.class, options::validateForCreate);
    }

    @Test
    void validateForCreate_continuousWithoutTo_passes() {
        ExportJobCreationOptions options = new ExportJobCreationOptions("job-1")
                .setMode(ExportMode.CONTINUOUS);
        options.validateForCreate();
    }
}
