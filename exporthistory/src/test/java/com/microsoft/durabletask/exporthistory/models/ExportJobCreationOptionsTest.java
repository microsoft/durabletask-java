// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExportJobCreationOptions} validation.
 */
class ExportJobCreationOptionsTest {

    private static final Instant NOW = Instant.now();
    private static final Instant ONE_HOUR_AGO = NOW.minusSeconds(3600);

    // region Mode validation

    @Test
    void batch_withCompletedTimeTo_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 100);
        assertEquals(ExportMode.BATCH, options.getMode());
        assertNotNull(options.getCompletedTimeTo());
    }

    @Test
    void batch_withoutCompletedTimeTo_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, null, null, null, null, 100));
    }

    @Test
    void continuous_withCompletedTimeTo_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.CONTINUOUS, ONE_HOUR_AGO, NOW, null, null, null, 100));
    }

    @Test
    void continuous_withoutCompletedTimeTo_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.CONTINUOUS, ONE_HOUR_AGO, null, null, null, null, 100);
        assertEquals(ExportMode.CONTINUOUS, options.getMode());
        assertNull(options.getCompletedTimeTo());
    }

    // endregion

    // region Time validation

    @Test
    void completedTimeTo_beforeCompletedTimeFrom_throws() {
        Instant before = ONE_HOUR_AGO.minusSeconds(3600);
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, before, null, null, null, 100));
    }

    @Test
    void completedTimeTo_equalToCompletedTimeFrom_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, ONE_HOUR_AGO, null, null, null, 100));
    }

    @Test
    void nullMode_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, null, ONE_HOUR_AGO, NOW, null, null, null, 100));
    }

    @Test
    void nullCompletedTimeFrom_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, null, NOW, null, null, null, 100));
    }

    // endregion

    // region Batch size validation

    @Test
    void maxInstancesPerBatch_zero_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 0));
    }

    @Test
    void maxInstancesPerBatch_tooLarge_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 1001));
    }

    @Test
    void maxInstancesPerBatch_minimum_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 1);
        assertEquals(1, options.getMaxInstancesPerBatch());
    }

    @Test
    void maxInstancesPerBatch_maximum_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 1000);
        assertEquals(1000, options.getMaxInstancesPerBatch());
    }

    // endregion

    // region Runtime status validation

    @Test
    void runtimeStatus_terminalOnly_succeeds() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null,
                Arrays.asList(OrchestrationRuntimeStatus.COMPLETED, OrchestrationRuntimeStatus.FAILED),
                100);
        assertEquals(2, options.getRuntimeStatus().size());
    }

    @Test
    void runtimeStatus_nonTerminal_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null,
                        Collections.singletonList(OrchestrationRuntimeStatus.RUNNING),
                        100));
    }

    @Test
    void runtimeStatus_pending_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new ExportJobCreationOptions(
                        null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null,
                        Collections.singletonList(OrchestrationRuntimeStatus.PENDING),
                        100));
    }

    // endregion

    // region Defaults

    @Test
    void jobId_autoGenerated_whenNull() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 100);
        assertNotNull(options.getJobId());
        assertFalse(options.getJobId().isEmpty());
    }

    @Test
    void jobId_preserved_whenProvided() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                "my-job-id", ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 100);
        assertEquals("my-job-id", options.getJobId());
    }

    @Test
    void format_defaultsToJsonl() {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                null, ExportMode.BATCH, ONE_HOUR_AGO, NOW, null, null, null, 100);
        assertEquals(ExportFormatKind.JSONL, options.getFormat().getKind());
        assertEquals("1.0", options.getFormat().getSchemaVersion());
    }

    // endregion
}
