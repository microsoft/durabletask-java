// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These integration tests are designed to exercise the core, high-level features of
 * the Durable Task programming model.
 * <p/>
 * These tests currently require a sidecar process to be
 * running on the local machine (the sidecar is what accepts the client operations and
 * sends invocation instructions to the DurableTaskWorker).
 */
@Tag("integration")
public class TmpTests extends IntegrationTestBase {
    static final Duration defaultTimeout = Duration.ofSeconds(100000);
    // All tests that create a server should save it to this variable for proper shutdown
    private DurableTaskGrpcWorker server;

    @AfterEach
    private void shutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Test
    void singleTimer() throws IOException, TimeoutException {
        final String orchestratorName = "SingleTimer";
        final Duration delay = Duration.ofSeconds(3);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).await())
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            Duration timeout = delay.plus(defaultTimeout);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, timeout, false);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            // Verify that the delay actually happened
            long expectedCompletionSecond = instance.getCreatedAt().plus(delay).getEpochSecond();
            long actualCompletionSecond = instance.getLastUpdatedAt().getEpochSecond();
            assertTrue(expectedCompletionSecond <= actualCompletionSecond);
        }
    }

    @Test()
    void waitForInstanceStartThrowsException() {
        final String orchestratorName = "waitForInstanceStartThrowsException";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                try {
                    // The orchestration remains in the "Pending" state until the first await statement
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            assertThrows(TimeoutException.class, () -> client.waitForInstanceStart(instanceId, Duration.ofSeconds(2)));
        }
    }
}