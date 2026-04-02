// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Demonstrates work item filtering with the Durable Task SDK.
 *
 * <p>Work item filters let a worker declare which orchestration and activity types it handles.
 * The backend then dispatches only matching work items to that worker, improving efficiency
 * in multi-worker deployments.
 *
 * <p>This sample starts two workers against the same task hub. Both workers register the
 * orchestration (orchestrations are deterministic so any worker can replay them), but each
 * worker handles a different subset of activities:
 * <ul>
 *   <li><b>Worker A</b> — handles "ValidateOrder" and "ProcessPayment" activities
 *       (uses explicit filters via {@link WorkItemFilter}).</li>
 *   <li><b>Worker B</b> — handles "ShipOrder" and "NotifyCustomer" activities
 *       (uses auto-generated filters via {@link DurableTaskGrpcWorkerBuilder#useWorkItemFilters()}).</li>
 * </ul>
 *
 * <p>A client schedules the orchestration and waits for it to complete, proving that the
 * backend correctly routes each work item to the appropriate worker.
 *
 * <h3>Running with the DTS emulator (default)</h3>
 * <pre>
 *   docker run -d -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
 *   ./gradlew :samples:runWorkItemFilterSample
 * </pre>
 *
 * <h3>Running with the standalone sidecar</h3>
 * <pre>
 *   # Start the sidecar on port 4001
 *   set USE_SIDECAR=true
 *   set SIDECAR_PORT=4001
 *   ./gradlew :samples:runWorkItemFilterSample
 * </pre>
 */
final class WorkItemFilterSample {
    private static final Logger logger = Logger.getLogger(WorkItemFilterSample.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        boolean useSidecar = Boolean.parseBoolean(System.getenv("USE_SIDECAR"));

        if (useSidecar) {
            runWithSidecar();
        } else {
            runWithDTS();
        }
    }

    // ---------------------------------------------------------------
    // DTS emulator path (Azure-managed Durable Task Scheduler)
    // ---------------------------------------------------------------
    private static void runWithDTS() throws IOException, InterruptedException, TimeoutException {
        String connectionString = System.getenv("DURABLE_TASK_CONNECTION_STRING");
        if (connectionString == null) {
            String endpoint = System.getenv("ENDPOINT");
            String taskHub = System.getenv("TASKHUB");
            if (endpoint == null) endpoint = "http://localhost:8080";
            if (taskHub == null) taskHub = "default";

            String authType = endpoint.startsWith("http://localhost") ? "None" : "DefaultAzure";
            connectionString = String.format("Endpoint=%s;TaskHub=%s;Authentication=%s",
                    endpoint, taskHub, authType);
        }
        logger.info("Using DTS connection string: " + connectionString);

        // --- Worker A: explicit filters -------------------------------------------------
        // Explicit filter declares exactly which task types Worker A can handle.
        WorkItemFilter workerAFilter = WorkItemFilter.newBuilder()
                .addOrchestration("ProcessOrder")
                .addActivity("ValidateOrder")
                .addActivity("ProcessPayment")
                .build();

        DurableTaskGrpcWorkerBuilder builderA =
                DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(connectionString);
        registerOrchestration(builderA);
        registerValidateOrder(builderA);
        registerProcessPayment(builderA);
        builderA.useWorkItemFilters(workerAFilter);
        DurableTaskGrpcWorker workerA = builderA.build();

        // --- Worker B: auto-generated filters -------------------------------------------
        // Filters are automatically derived from the registered orchestrations and activities.
        DurableTaskGrpcWorkerBuilder builderB =
                DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(connectionString);
        registerOrchestration(builderB);
        registerShipOrder(builderB);
        registerNotifyCustomer(builderB);
        builderB.useWorkItemFilters(); // auto-generate from registered tasks
        DurableTaskGrpcWorker workerB = builderB.build();

        // --- Client ---------------------------------------------------------------------
        DurableTaskClient client = DurableTaskSchedulerClientExtensions
                .createClientBuilder(connectionString).build();

        runSample(workerA, workerB, client);
    }

    // ---------------------------------------------------------------
    // Standalone sidecar path (plain gRPC, e.g. Dapr or emulator)
    // ---------------------------------------------------------------
    private static void runWithSidecar() throws IOException, InterruptedException, TimeoutException {
        int port = 4001;
        String portEnv = System.getenv("SIDECAR_PORT");
        if (portEnv != null) {
            port = Integer.parseInt(portEnv);
        }
        logger.info("Using standalone sidecar on port " + port);

        // --- Worker A: explicit filters -------------------------------------------------
        WorkItemFilter workerAFilter = WorkItemFilter.newBuilder()
                .addOrchestration("ProcessOrder")
                .addActivity("ValidateOrder")
                .addActivity("ProcessPayment")
                .build();

        DurableTaskGrpcWorkerBuilder builderA = new DurableTaskGrpcWorkerBuilder().port(port);
        registerOrchestration(builderA);
        registerValidateOrder(builderA);
        registerProcessPayment(builderA);
        builderA.useWorkItemFilters(workerAFilter);
        DurableTaskGrpcWorker workerA = builderA.build();

        // --- Worker B: auto-generated filters -------------------------------------------
        DurableTaskGrpcWorkerBuilder builderB = new DurableTaskGrpcWorkerBuilder().port(port);
        registerOrchestration(builderB);
        registerShipOrder(builderB);
        registerNotifyCustomer(builderB);
        builderB.useWorkItemFilters(); // auto-generate from registered tasks
        DurableTaskGrpcWorker workerB = builderB.build();

        // --- Client ---------------------------------------------------------------------
        DurableTaskClient client = new DurableTaskGrpcClientBuilder().port(port).build();

        runSample(workerA, workerB, client);
    }

    // ---------------------------------------------------------------
    // Shared orchestration / activity logic
    // ---------------------------------------------------------------
    private static void runSample(
            DurableTaskGrpcWorker workerA,
            DurableTaskGrpcWorker workerB,
            DurableTaskClient client) throws InterruptedException, TimeoutException {

        workerA.start();
        workerB.start();
        logger.info("Both workers started.");

        // Schedule an orchestration
        String instanceId = client.scheduleNewOrchestrationInstance(
                "ProcessOrder",
                new NewOrchestrationInstanceOptions().setInput("order-12345"));
        logger.info("Scheduled orchestration: " + instanceId);

        // Wait for completion
        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(60), true);

        logger.info("Orchestration completed!");
        logger.info("  Status : " + result.getRuntimeStatus());
        logger.info("  Output : " + result.readOutputAs(String.class));

        // Clean up
        workerA.stop();
        workerB.stop();
        logger.info("Workers stopped. Sample finished.");
        System.exit(0);
    }

    // ---------------------------------------------------------------
    // Task registrations
    // ---------------------------------------------------------------

    /** The orchestration calls four activities across two workers. */
    private static void registerOrchestration(DurableTaskGrpcWorkerBuilder builder) {
        builder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ProcessOrder"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String orderId = ctx.getInput(String.class);
                    logger.info("[Orchestrator] Processing order " + orderId);

                    // Step 1 — handled by Worker A
                    boolean valid = ctx.callActivity("ValidateOrder", orderId, Boolean.class).await();
                    if (!valid) {
                        ctx.complete("Order " + orderId + " validation failed.");
                        return;
                    }

                    // Step 2 — handled by Worker A
                    String paymentId = ctx.callActivity("ProcessPayment", orderId, String.class).await();

                    // Step 3 — handled by Worker B
                    String trackingNumber = ctx.callActivity("ShipOrder", orderId, String.class).await();

                    // Step 4 — handled by Worker B
                    ctx.callActivity("NotifyCustomer", orderId, String.class).await();

                    ctx.complete(String.format(
                            "Order %s completed. Payment=%s, Tracking=%s",
                            orderId, paymentId, trackingNumber));
                };
            }
        });
    }

    /** Activity on Worker A: validates the order. */
    private static void registerValidateOrder(DurableTaskGrpcWorkerBuilder builder) {
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ValidateOrder"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String orderId = ctx.getInput(String.class);
                    logger.info("[Worker A] ValidateOrder for " + orderId);
                    return true;
                };
            }
        });
    }

    /** Activity on Worker A: processes payment. */
    private static void registerProcessPayment(DurableTaskGrpcWorkerBuilder builder) {
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ProcessPayment"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String orderId = ctx.getInput(String.class);
                    logger.info("[Worker A] ProcessPayment for " + orderId);
                    return "PAY-" + orderId.hashCode();
                };
            }
        });
    }

    /** Activity on Worker B: ships the order. */
    private static void registerShipOrder(DurableTaskGrpcWorkerBuilder builder) {
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ShipOrder"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String orderId = ctx.getInput(String.class);
                    logger.info("[Worker B] ShipOrder for " + orderId);
                    return "TRACK-" + orderId.hashCode();
                };
            }
        });
    }

    /** Activity on Worker B: notifies the customer. */
    private static void registerNotifyCustomer(DurableTaskGrpcWorkerBuilder builder) {
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "NotifyCustomer"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String orderId = ctx.getInput(String.class);
                    logger.info("[Worker B] NotifyCustomer for " + orderId);
                    return "Customer notified for order " + orderId;
                };
            }
        });
    }
}
