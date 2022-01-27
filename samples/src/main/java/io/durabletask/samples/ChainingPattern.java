// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;

final class ChainingPattern {
    public static void main(String[] args) throws IOException, InterruptedException {
        // The TaskHubServer listens over gRPC for new orchestration and activity execution requests
        final DurableTaskGrpcWorker server = createTaskHubServer();

        // Start the server to begin processing orchestration and activity requests
        server.start();

        // Start a new instance of the registered "ActivityChaining" orchestration
        final DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(
                "ActivityChaining",
                NewOrchestrationInstanceOptions.newBuilder().setInput("Hello, world!").build());
        System.out.printf("Started new orchestration instance: %s%n", instanceId);

        // Block until the orchestration completes. Then print the final status, which includes the output.
        OrchestrationMetadata completedInstance = client.waitForInstanceCompletion(
                instanceId,
                Duration.ofSeconds(30),
                true);
        System.out.printf("Orchestration completed: %s%n", completedInstance);
        System.out.printf("Output: %s%n", completedInstance.readOutputAs(String.class));

        // Shutdown the server and exit
        server.stop();
    }

    // class OrderInfo {}

    // class ServicesAPIs {
    //     public static final String UpdateInventory = "";
    // }

    // class RetryPolicy {
    //     public RetryPolicy(int maxAttempts, Duration retryInterval) {

    //     }
    // }

    private static DurableTaskGrpcWorker createTaskHubServer() {
        DurableTaskGrpcWorker.Builder builder = DurableTaskGrpcWorker.newBuilder();

        // Orchestrations can be defined inline as anonymous classes or as concrete classes
        builder.addOrchestration(new TaskOrchestrationFactory() {
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

                    // Save the output of the orchestration
                    ctx.complete(z);

                    // var order = ctx.getInput(OrderInfo.class); // deserialize order info from JSON to an object
                    // var retryPolicy = RetryPolicy.newBuilder()
                    //         .setMaxRetries(100)
                    //         .setRetryInterval(Duration.ofSeconds(30));

                    // // Remove the inventory
                    // var itemDetails = ctx.callActivity("UpdateInventory", order.ShoppingCart, retryPolicy).get();
                    // var paymentSummary = ctx.callActivity("ProcessPayment", order.PaymentDetails, retryPolicy).get();
                    // ctx.callActivity("ShipInventory", itemDetails, retryPolicy).get();

                    // var orderSummary = getOrderSummary(itemDetails, paymentSummary);
                    // ctx.callActivity("SendConfirmationEmail", itemDetails, retryPolicy).get();
                };
            }
        });

        // Activities can be defined inline as anonymous classes or as concrete classes
        builder.addActivity(new TaskActivityFactory() {
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

        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Capitalize"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(String.class).toUpperCase();
            }
        });

        builder.addActivity(new TaskActivityFactory() {
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

        return builder.build();
    }
}