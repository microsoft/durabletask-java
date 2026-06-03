// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrates {@link TaskOrchestrationContext#getParentInstance()}: the child orchestration
 * reports its parent when started as a sub-orchestration, and reports {@code null} when started
 * standalone.
 *
 * <p>Run with: {@code ./gradlew runParentInstanceSample}
 */
final class ParentInstanceSample {
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorkerBuilder workerBuilder = SampleUtils.newWorkerBuilder();

        // Parent orchestration: starts the child as a sub-orchestration.
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ParentOrchestrator"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    log(ctx, "Starting child sub-orchestration...");
                    String childResult = ctx.callSubOrchestrator(
                            "ChildOrchestrator", null, String.class).await();
                    log(ctx, "Child returned: " + childResult);
                    ctx.complete(childResult);
                };
            }
        });

        // Child orchestration: reads getParentInstance() directly on the context.
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ChildOrchestrator"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String result = describeParent(ctx.getParentInstance());
                    log(ctx, result);
                    ctx.complete(result);
                };
            }
        });

        DurableTaskGrpcWorker worker = workerBuilder.build();
        worker.start();

        DurableTaskClient client = SampleUtils.newClientBuilder().build();

        // Step 1: Start the parent. The child observes its parent via getParentInstance().
        System.out.println("=== Step 1: Sub-orchestration (child has parent) ===");
        String parentId = client.scheduleNewOrchestrationInstance("ParentOrchestrator");
        System.out.println("  Scheduled ParentOrchestrator: " + parentId);
        OrchestrationMetadata result1 = client.waitForInstanceCompletion(
                parentId, Duration.ofSeconds(30), true);
        System.out.println("  Status: " + result1.getRuntimeStatus());
        if (result1.getRuntimeStatus() == OrchestrationRuntimeStatus.COMPLETED) {
            System.out.println("  Result: " + result1.readOutputAs(String.class));
        } else {
            System.out.println("  Failure: " + result1.getFailureDetails());
        }

        // Step 2: Start the child directly. getParentInstance() returns null.
        System.out.println("\n=== Step 2: Standalone orchestration (no parent) ===");
        String standaloneId = client.scheduleNewOrchestrationInstance("ChildOrchestrator");
        System.out.println("  Scheduled ChildOrchestrator standalone: " + standaloneId);
        OrchestrationMetadata result2 = client.waitForInstanceCompletion(
                standaloneId, Duration.ofSeconds(30), true);
        System.out.println("  Result: " + result2.readOutputAs(String.class));

        System.out.println("\n=== Sample completed ===");
        worker.stop();
    }

    /** Returns a string describing the given parent instance, or that there is none. */
    private static String describeParent(@Nullable ParentOrchestrationInstance parent) {
        if (parent != null) {
            return String.format("I was called by '%s' (instance: %s)",
                    parent.getName(), parent.getInstanceId());
        }
        return "No parent — I was started standalone";
    }

    /** Replay-safe console log. */
    private static void log(TaskOrchestrationContext ctx, String message) {
        if (!ctx.getIsReplaying()) {
            System.out.println("  [" + ctx.getName() + "] " + message);
        }
    }
}
