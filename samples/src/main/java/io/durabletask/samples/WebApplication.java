// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
 
@SpringBootApplication
public class WebApplication {
 
    public static void main(String[] args) throws InterruptedException {
        DurableTaskGrpcWorker server = createTaskHubServer();
        server.start();

        System.out.println("Starting up Spring web API...");
        SpringApplication.run(WebApplication.class, args);
    }

    private static DurableTaskGrpcWorker createTaskHubServer() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        
        // Orchestrations can be defined inline as anonymous classes or as concrete classes
        builder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ProcessOrderOrchestration"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // the input is JSON and can be deserialized into the specified type
                    String input = ctx.getInput(String.class);

                    String x = ctx.callActivity("Task1", input, String.class).await();
                    String y = ctx.callActivity("Task2", x, String.class).await();
                    String z = ctx.callActivity("Task3", y, String.class).await();

                    ctx.complete(z);
                };
            }
        });

        // Activities can be defined inline as anonymous classes or as concrete classes
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Task1"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    sleep(10000);
                    return input + "|" + ctx.getName();
                };
            }
        });

        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Task2"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    sleep(10000);
                    return input + "|" + ctx.getName();
                };
            }
        });

        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Task3"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    sleep(10000);
                    return input + "|" + ctx.getName();
                };
            }
        });

        return builder.build();
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
