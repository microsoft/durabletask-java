// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.client.InstanceIdReuseAction;
import io.grpc.StatusRuntimeException;
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
@Tag("integration-go")
public class IntegrationGoTests extends IntegrationTestBase {
    static final Duration defaultTimeout = Duration.ofSeconds(100);
    // All tests that create a server should save it to this variable for proper shutdown
    private DurableTaskGrpcWorker server;

    @AfterEach
    private void shutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Test
    void singleActivityIgnore() throws TimeoutException {
        final String orchestratorName = "SingleActivity";
        final String activityName = "Echo";
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                String activityInput = ctx.getInput(String.class);
                ctx.createTimer(Duration.ofSeconds(2));
                String output = ctx.callActivity(activityName, activityInput, String.class).await();
                ctx.complete(output);
            })
            .addActivity(activityName, ctx -> {
                return String.format("Hello, %s!", ctx.getInput(String.class));
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            final String instanceID = "SKIP_IF_RUNNING_OR_COMPLETED";
            NewOrchestrationInstanceOptions instanceOptions = new NewOrchestrationInstanceOptions();
            instanceOptions
                .setInstanceId(instanceID)
                .setInput("World")
                .addTargetStatus(OrchestrationRuntimeStatus.RUNNING, OrchestrationRuntimeStatus.COMPLETED, OrchestrationRuntimeStatus.PENDING)
                .setInstanceIdReuseAction(InstanceIdReuseAction.IGNORE);

            client.scheduleNewOrchestrationInstance(orchestratorName, "GO", instanceID);
            client.waitForInstanceStart(instanceID, defaultTimeout);
            long pivotTime = Instant.now().getEpochSecond();
            client.scheduleNewOrchestrationInstance(orchestratorName, instanceOptions);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceID,
                defaultTimeout,
                true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            String output = instance.readOutputAs(String.class);
            String expected = "Hello, GO!";
            assertEquals(expected, output);

            // Verify that the delay actually happened
            long expectedCompletionSecond = instance.getCreatedAt().getEpochSecond();
            assertTrue(expectedCompletionSecond <= pivotTime);
        }
    }

    @Test
    void singleActivityTerminate() throws TimeoutException {
        final String orchestratorName = "SingleActivity";
        final String activityName = "Echo";
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                String activityInput = ctx.getInput(String.class);
                ctx.createTimer(Duration.ofSeconds(2));
                String output = ctx.callActivity(activityName, activityInput, String.class).await();
                ctx.complete(output);
            })
            .addActivity(activityName, ctx -> {
                return String.format("Hello, %s!", ctx.getInput(String.class));
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            final String instanceID = "TERMINATE_IF_RUNNING_OR_COMPLETED";
            NewOrchestrationInstanceOptions instanceOptions = new NewOrchestrationInstanceOptions();
            instanceOptions
                .setInstanceId(instanceID)
                .setInput("World")
                .addTargetStatus(OrchestrationRuntimeStatus.RUNNING, OrchestrationRuntimeStatus.COMPLETED, OrchestrationRuntimeStatus.PENDING)
                .setInstanceIdReuseAction(InstanceIdReuseAction.TERMINATE);

            client.scheduleNewOrchestrationInstance(orchestratorName, "GO", instanceID);
            client.waitForInstanceStart(instanceID, defaultTimeout);
            long pivotTime = Instant.now().getEpochSecond();
            client.scheduleNewOrchestrationInstance(orchestratorName, instanceOptions);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceID,
                defaultTimeout,
                true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            String output = instance.readOutputAs(String.class);
            String expected = "Hello, World!";
            assertEquals(expected, output);

            // Verify that the delay actually happened
            long expectedCompletionSecond = instance.getCreatedAt().getEpochSecond();
            assertTrue(pivotTime <= expectedCompletionSecond);
        }
    }

    @Test
    void singleActivityError() throws TimeoutException {
        final String orchestratorName = "SingleActivity";
        final String activityName = "Echo";
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                String activityInput = ctx.getInput(String.class);
                ctx.createTimer(Duration.ofSeconds(2));
                String output = ctx.callActivity(activityName, activityInput, String.class).await();
                ctx.complete(output);
            })
            .addActivity(activityName, ctx -> {
                return String.format("Hello, %s!", ctx.getInput(String.class));
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            final String instanceID = "ERROR_IF_RUNNING_OR_COMPLETED";

            client.scheduleNewOrchestrationInstance(orchestratorName, "GO", instanceID);
            assertThrows(
                StatusRuntimeException.class,
                () -> client.scheduleNewOrchestrationInstance(orchestratorName, "World", instanceID)
            );
        }
    }
}