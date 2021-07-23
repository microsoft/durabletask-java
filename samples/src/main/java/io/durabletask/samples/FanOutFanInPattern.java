// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

class FanOutFanInPattern {

    public static void main(String[] args) throws IOException, InterruptedException {
        // The TaskHubServer listens over gRPC for new orchestration and activity execution requests
        final TaskHubServer server = createTaskHubServer();

        // Start the server to begin processing orchestration and activity requests
        server.start();

        // Start a new instance of the registered "ActivityChaining" orchestration
        final TaskHubClient client = TaskHubClient.newBuilder().build();

        // The input is an arbitrary list of strings.
        List<String> listOfStrings = List.of(
                "Hello, world!",
                "The quick brown fox jumps over the lazy dog.",
                "If a tree falls in the forest and there is no one there to hear it, does it make a sound?",
                "The greatest glory in living lies not in never falling, but in rising every time we fall.",
                "Always remember that you are absolutely unique. Just like everyone else.");

        // Schedule an orchestration which will reliably count the number of words in all the given sentences.
        String instanceId = client.scheduleNewOrchestrationInstance(
                "FanOutFanIn_WordCount",
                NewOrchestrationInstanceOptions.newBuilder().setInput(listOfStrings).build());
        System.out.printf("Started new orchestration instance: %s%n", instanceId);

        // Block until the orchestration completes. Then print the final status, which includes the output.
        TaskOrchestrationInstance completedInstance = client.waitForInstanceCompletion(
                instanceId,
                Duration.ofSeconds(30),
                true);
        System.out.printf("Orchestration completed: %s%n", completedInstance);
        System.out.printf("Output: %d%n", completedInstance.getOutputAs(int.class));

        // Shutdown the server and exit
        server.stop();
    }

    private static TaskHubServer createTaskHubServer() {
        TaskHubServer.Builder builder = TaskHubServer.newBuilder();

        // Orchestrations can be defined inline as anonymous classes or as concrete classes
        builder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "FanOutFanIn_WordCount"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // The input is a list of objects that need to be operated on.
                    // In this example, inputs are expected to be strings.
                    List<?> inputs = ctx.getInput(List.class);

                    // Fan-out to multiple concurrent activity invocations, each of which does a word count.
                    List<Task<Integer>> tasks = inputs.stream()
                            .map(input -> ctx.callActivity("CountWords", input.toString(), Integer.class))
                            .collect(Collectors.toList());

                    // Fan-in to get the total word count from all the individual activity results.
                    List<Integer> allWordCountResults = ctx.allOf(tasks).get();
                    int totalWordCount = allWordCountResults.stream().mapToInt(Integer::intValue).sum();

                    // Save the final result as the orchestration output.
                    ctx.complete(totalWordCount);
                };
            }
        });

        // Activities can be defined inline as anonymous classes or as concrete classes
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "CountWords"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    // Take the string input and count the number of words (tokens) it contains.
                    String input = ctx.getInput(String.class);
                    StringTokenizer tokenizer = new StringTokenizer(input);
                    return tokenizer.countTokens();
                };
            }
        });

        return builder.build();
    }
}
