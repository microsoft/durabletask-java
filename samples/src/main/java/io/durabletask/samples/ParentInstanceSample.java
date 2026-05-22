// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;
import lombok.experimental.Delegate;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrates how to wrap {@link TaskOrchestrationContext} and forward
 * {@link TaskOrchestrationContext#getParentInstance()} through the wrapper so that
 * helper methods built on top of the wrapped context can observe the parent
 * orchestration (if any).
 *
 * <p>This mirrors the .NET {@code ReplaySafeLoggerFactorySample} pattern, where the
 * wrapping context explicitly forwards the abstract {@code Parent} property.
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
                    LoggingTaskOrchestrationContext wrapped = new LoggingTaskOrchestrationContext(ctx);
                    wrapped.log("Starting child sub-orchestration...");
                    String childResult = wrapped.callSubOrchestrator(
                            "ChildOrchestrator", null, String.class).await();
                    wrapped.log("Child returned: " + childResult);
                    wrapped.complete(childResult);
                };
            }
        });

        // Child orchestration: uses the wrapper helper that reports the parent.
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ChildOrchestrator"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    LoggingTaskOrchestrationContext wrapped = new LoggingTaskOrchestrationContext(ctx);
                    String result = wrapped.describeParent();
                    wrapped.log(result);
                    wrapped.complete(result);
                };
            }
        });

        DurableTaskGrpcWorker worker = workerBuilder.build();
        worker.start();

        DurableTaskClient client = SampleUtils.newClientBuilder().build();

        // Step 1: Start the parent. The child observes its parent through the wrapper.
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

    /**
     * Wrapper that forwards every {@link TaskOrchestrationContext} member to an inner
     * context and exposes replay-safe helpers built on top of it.
     *
     * <p>Because {@link TaskOrchestrationContext#getParentInstance()} is abstract,
     * any wrapper must explicitly forward it. Here we let Lombok {@code @Delegate}
     * generate the forwarding for every method, including {@code getParentInstance()}.
     * The {@link #describeParent()} and {@link #log(String)} helpers then build on
     * the wrapped context without breaking replay safety.
     */
    static final class LoggingTaskOrchestrationContext implements TaskOrchestrationContext {
        @Delegate(types = TaskOrchestrationContext.class)
        private final TaskOrchestrationContext inner;

        LoggingTaskOrchestrationContext(TaskOrchestrationContext inner) {
            if (inner == null) {
                throw new IllegalArgumentException("inner must not be null");
            }
            this.inner = inner;
        }

        /** Returns a string describing this orchestration's parent, or that it has none. */
        String describeParent() {
            @Nullable ParentOrchestrationInstance parent = inner.getParentInstance();
            if (parent != null) {
                return String.format("I was called by '%s' (instance: %s)",
                        parent.getName(), parent.getInstanceId());
            }
            return "No parent — I was started standalone";
        }

        /** Replay-safe console log. */
        void log(String message) {
            if (!inner.getIsReplaying()) {
                System.out.println("  [" + inner.getName() + "] " + message);
            }
        }
    }
}
