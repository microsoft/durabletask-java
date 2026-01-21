// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sample demonstrating the rewind functionality.
 * 
 * Rewind allows a failed orchestration to be replayed from its last known good state.
 * This is useful for recovering from transient failures without losing progress.
 * 
 * This sample:
 * 1. Starts an orchestration that calls an activity which fails on the first attempt
 * 2. Waits for the orchestration to fail
 * 3. Rewinds the orchestration, which replays it from the failure point
 * 4. The activity succeeds on retry, and the orchestration completes
 */
final class RewindPattern {
    
    // Flag to simulate a transient failure (fails first time, succeeds after)
    private static final AtomicBoolean shouldFail = new AtomicBoolean(true);

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        // Create and start the worker
        final DurableTaskGrpcWorker worker = createTaskHubWorker();
        worker.start();

        // Create the client
        final DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        try {
            // Reset the failure flag
            shouldFail.set(true);

            // Start the orchestration - it will fail on the first activity call
            String instanceId = client.scheduleNewOrchestrationInstance(
                    "RewindableOrchestration",
                    new NewOrchestrationInstanceOptions().setInput("TestInput"));
            System.out.printf("Started orchestration instance: %s%n", instanceId);

            // Wait for the orchestration to fail
            System.out.println("Waiting for orchestration to fail...");
            OrchestrationMetadata failedInstance = client.waitForInstanceCompletion(
                    instanceId,
                    Duration.ofSeconds(30),
                    true);
            
            System.out.printf("Orchestration status: %s%n", failedInstance.getRuntimeStatus());
            
            if (failedInstance.getRuntimeStatus() == OrchestrationRuntimeStatus.FAILED) {
                System.out.println("Orchestration failed as expected. Now rewinding...");
                
                // Rewind the failed orchestration
                client.rewindInstance(instanceId, "Rewinding after transient failure");
                System.out.println("Rewind request sent.");

                // Wait for the orchestration to complete after rewind
                System.out.println("Waiting for orchestration to complete after rewind...");
                OrchestrationMetadata completedInstance = client.waitForInstanceCompletion(
                        instanceId,
                        Duration.ofSeconds(30),
                        true);
                
                System.out.printf("Orchestration completed: %s%n", completedInstance.getRuntimeStatus());
                System.out.printf("Output: %s%n", completedInstance.readOutputAs(String.class));
            } else {
                System.out.println("Unexpected status: " + failedInstance.getRuntimeStatus());
            }

        } finally {
            // Shutdown the worker
            worker.stop();
        }
    }

    private static DurableTaskGrpcWorker createTaskHubWorker() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();

        // Register the orchestration
        builder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { 
                return "RewindableOrchestration"; 
            }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    
                    // Call an activity that may fail
                    String result = ctx.callActivity("FailOnceActivity", input, String.class).await();
                    
                    ctx.complete(result);
                };
            }
        });

        // Register the activity that fails on first call
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { 
                return "FailOnceActivity"; 
            }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    
                    // Fail on the first call, succeed on subsequent calls
                    if (shouldFail.compareAndSet(true, false)) {
                        System.out.println("FailOnceActivity: Simulating transient failure...");
                        throw new RuntimeException("Simulated transient failure - rewind to retry");
                    }
                    
                    System.out.println("FailOnceActivity: Succeeded after rewind!");
                    return input + "-rewound-success";
                };
            }
        });

        return builder.build();
    }
}
