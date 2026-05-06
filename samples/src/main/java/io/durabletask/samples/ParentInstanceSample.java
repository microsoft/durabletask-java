// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrates the {@link TaskOrchestrationContext#getParent()} API.
 *
 * <p>A parent orchestration starts a child sub-orchestration. The child uses
 * {@code ctx.getParent()} to discover its parent's name and instance ID.
 * The same child orchestration is also started standalone to show that
 * {@code getParent()} returns {@code null} in that case.
 *
 * <p>Run with: {@code ./gradlew runParentInstanceSample}
 */
final class ParentInstanceSample {
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorkerBuilder workerBuilder = SampleUtils.newWorkerBuilder();

        // --- Register orchestrations ---

        // Parent orchestration: calls ChildOrchestrator as a sub-orchestration
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ParentOrchestrator"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    if (!ctx.getIsReplaying()) {
                        System.out.println("  [Parent] Starting child sub-orchestration...");
                    }
                    String childResult = ctx.callSubOrchestrator(
                            "ChildOrchestrator", null, String.class).await();
                    if (!ctx.getIsReplaying()) {
                        System.out.println("  [Parent] Child returned: " + childResult);
                    }
                    ctx.complete(childResult);
                };
            }
        });

        // Child orchestration: reports its parent (if any)
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ChildOrchestrator"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ParentOrchestrationInstance parent = ctx.getParent();
                    String result;
                    if (parent != null) {
                        result = String.format("I was called by '%s' (instance: %s)",
                                parent.getName(), parent.getInstanceId());
                        if (!ctx.getIsReplaying()) {
                            System.out.println("  [Child] " + result);
                        }
                    } else {
                        result = "No parent — I was started standalone";
                        if (!ctx.getIsReplaying()) {
                            System.out.println("  [Child] " + result);
                        }
                    }
                    ctx.complete(result);
                };
            }
        });

        DurableTaskGrpcWorker worker = workerBuilder.build();
        worker.start();

        DurableTaskClient client = SampleUtils.newClientBuilder().build();

        // --- Step 1: Start parent orchestration (child will see its parent) ---
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

        // --- Step 2: Start child orchestration directly (no parent) ---
        System.out.println("\n=== Step 2: Standalone orchestration (no parent) ===");
        String standaloneId = client.scheduleNewOrchestrationInstance("ChildOrchestrator");
        System.out.println("  Scheduled ChildOrchestrator standalone: " + standaloneId);
        OrchestrationMetadata result2 = client.waitForInstanceCompletion(
                standaloneId, Duration.ofSeconds(30), true);
        System.out.println("  Result: " + result2.readOutputAs(String.class));

        System.out.println("\n=== Sample completed ===");
        worker.stop();
    }
}
