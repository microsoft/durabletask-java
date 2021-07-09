// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;

final class ChainingPattern {
    public static void main(String[] args) throws IOException, InterruptedException {
        // The TaskHubServer listens over gRPC for new orchestration and activity execution requests
        final TaskHubServer server = TaskHubServer.newBuilder().build();

        // Orchestration and activity tasks must be registered with the server
        registerDurableTasks(server);

        // Start the server to begin processing orchestration and activity requests
        server.start();

        // Start a new instance of the registered "ActivityChaining" orchestration
        final TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(
                "ActivityChaining",
                NewOrchestrationInstanceOptions.newBuilder().setInput("Hello, world!").build());
        System.out.printf("Started new orchestration instance: %s%n", instanceId);

        // Block until the orchestration completes. Then print the final status, which includes the output.
        TaskOrchestrationInstance completedInstance = client.waitForInstanceCompletion(
                instanceId,
                Duration.ofSeconds(30),
                true);
        System.out.printf("Orchestration completed: %s%n", completedInstance);
        System.out.printf("Output: %s%n", completedInstance.getOutputAs(String.class));

        // Shutdown the server and exit
        server.stop();
    }

    private static void registerDurableTasks(TaskHubServer server) {
        // Orchestrations can be defined inline as anonymous classes or as concrete classes
        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ActivityChaining"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // the input is JSON and can be deserialized into the specified type
                    String input = ctx.getInput(String.class);

                    String x = ctx.callActivity("Reverse", input, String.class).get();
                    String y = ctx.callActivity("Capitalize", x, String.class).get();
                    String z = ctx.callActivity("ReplaceWhitespace", y, String.class).get();

                    ctx.complete(z);
                };
            }
        });

        // Activities can be defined inline as anonymous classes or as concrete classes
        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Reverse"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    StringBuilder builder = new StringBuilder(input);
                    builder.reverse();
                    return builder.toString();
                };
            }
        });

        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Capitalize"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(String.class).toUpperCase();
            }
        });

        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ReplaceWhitespace"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    return input.trim().replaceAll("\\s", "-");
                };
            }
        });
    }
}