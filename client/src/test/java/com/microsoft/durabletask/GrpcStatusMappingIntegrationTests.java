// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that verify gRPC {@code StatusRuntimeException} codes are correctly
 * translated into standard Java exceptions at the SDK boundary.
 * <p/>
 * These tests require the DTS emulator sidecar to be running on localhost:4001.
 */
@Tag("integration")
public class GrpcStatusMappingIntegrationTests extends IntegrationTestBase {

    // -----------------------------------------------------------------------
    // NOT_FOUND → IllegalArgumentException
    // -----------------------------------------------------------------------

    @Test
    void raiseEvent_nonExistentInstance_throwsIllegalArgumentException() {
        DurableTaskClient client = this.createClientBuilder().build();
        try (client) {
            // The emulator returns NOT_FOUND when raising an event on an instance that doesn't exist.
            // The SDK should translate this to IllegalArgumentException.
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                    client.raiseEvent("definitely-missing-id", "testEvent", null));
            assertNotNull(ex.getCause(), "Should preserve original gRPC exception as cause");
        }
    }

    @Test
    void suspendInstance_nonExistentInstance_throwsIllegalArgumentException() {
        DurableTaskClient client = this.createClientBuilder().build();
        try (client) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                    client.suspendInstance("definitely-missing-id", "test suspend"));
            assertNotNull(ex.getCause(), "Should preserve original gRPC exception as cause");
        }
    }

    @Test
    void terminateInstance_nonExistentInstance_throwsIllegalArgumentException() {
        DurableTaskClient client = this.createClientBuilder().build();
        try (client) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                    client.terminate("definitely-missing-id", "test terminate"));
            assertNotNull(ex.getCause(), "Should preserve original gRPC exception as cause");
        }
    }

    // -----------------------------------------------------------------------
    // NOT_FOUND: getInstanceMetadata returns isInstanceFound==false (no throw)
    // -----------------------------------------------------------------------

    @Test
    void getInstanceMetadata_nonExistentInstance_returnsNotFound() {
        DurableTaskClient client = this.createClientBuilder().build();
        try (client) {
            // The DTS emulator returns an empty response (not NOT_FOUND gRPC status) for
            // missing instances, so getInstanceMetadata does not throw — it returns metadata
            // with isInstanceFound == false.
            OrchestrationMetadata metadata = client.getInstanceMetadata("definitely-missing-id", false);
            assertNotNull(metadata);
            assertFalse(metadata.isInstanceFound());
        }
    }

    // -----------------------------------------------------------------------
    // DEADLINE_EXCEEDED → TimeoutException
    // -----------------------------------------------------------------------

    @Test
    void waitForInstanceCompletion_tinyTimeout_throwsTimeoutException() throws TimeoutException {
        final String orchestratorName = "SlowOrchestrator";

        // Orchestrator that waits far longer than our timeout
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    ctx.createTimer(Duration.ofMinutes(10)).await();
                    ctx.complete("done");
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            // Wait for the instance to actually start before applying the tiny timeout
            client.waitForInstanceStart(instanceId, defaultTimeout, false);

            Duration tinyTimeout = Duration.ofSeconds(1);
            assertThrows(TimeoutException.class, () ->
                    client.waitForInstanceCompletion(instanceId, tinyTimeout, false));
        }
    }

    // -----------------------------------------------------------------------
    // UNIMPLEMENTED → UnsupportedOperationException (rewind not supported by emulator)
    // -----------------------------------------------------------------------

    @Test
    void rewindInstance_throwsUnsupportedOperationException() {
        DurableTaskClient client = this.createClientBuilder().build();
        try (client) {
            // The DTS emulator does not support the rewind operation and returns UNIMPLEMENTED.
            // The SDK should translate this to UnsupportedOperationException.
            UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class, () ->
                    client.rewindInstance("any-instance-id", "test rewind"));
            assertNotNull(ex.getCause(), "Should preserve original gRPC exception as cause");
            assertTrue(ex.getMessage().contains("UNIMPLEMENTED"));
        }
    }
}
