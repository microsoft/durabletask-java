// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.protobuf.TaskHubWorkerServiceGrpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskHubServer {
    private static final int DEFAULT_PORT = 4000;

    private static final Logger logger = Logger.getLogger(TaskHubServer.class.getPackage().getName());
    private Server grpcServer;

    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();

    private final int port;
    private final DataConverter dataConverter;

    private TaskHubServer(Builder builder) {
        if (builder.port > 0) {
            this.port = builder.port;
        } else {
            this.port = DEFAULT_PORT;
        }

        this.dataConverter = Objects.requireNonNullElse(builder.dataConverter, new JacksonDataConverter());
    }

    // TODO: Put these methods behind a builder abstraction
    public void addOrchestration(TaskOrchestrationFactory factory) {
        // TODO: Input validation
        String key = factory.getName();
        if (this.orchestrationFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                String.format("A task orchestration factory named %s is already registered.", key));
        }

        this.orchestrationFactories.put(key, factory);
    }

    public void addActivity(TaskActivityFactory factory) {
        // TODO: Input validation
        String key = factory.getName();
        if (this.activityFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("A task activity factory named %s is already registered.", key));
        }

        this.activityFactories.put(key, factory);
    }

    public void start() throws IOException {
        this.grpcServer = ServerBuilder
            .forPort(this.port)
            .addService((new TaskHubWorkerServiceImpl()))
            .build()
            .start();
        logger.info("Server started, listening on " + this.port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("Shutting down gRPC server since JVM is shutting down...");
            try {
                TaskHubServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("gRPC server shutdown completed.");
        }));
    }

    public void stop() throws InterruptedException {
        if (this.grpcServer != null) {
            this.grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (this.grpcServer != null) {
            this.grpcServer.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final TaskHubServer server = TaskHubServer.newBuilder().build();
        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ActivityChaining"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    int initial = ctx.getInput(int.class);

                    int x = ctx.callActivity("PlusOne", initial, int.class).get();
                    int y = ctx.callActivity("PlusOne", x, int.class).get();
                    int z = ctx.callActivity("PlusOne", y, int.class).get();

                    ctx.complete(z);
                };
            }
        });
        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "PlusOne"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(int.class) + 1;
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return "Test";
            }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String output = String.format("Finished '%s', ID = %s", ctx.getName(), ctx.getInstanceId());
                    ctx.complete(output);
                };
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return "OrchestrationWithTimer";
            }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    Task<Void> timer = ctx.createTimer(Duration.ofSeconds(3));
                    timer.thenRun(() -> ctx.complete(ctx.getInput(Object.class)));
                };
            }
        });
        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return "OrchestrationWithTimer2";
            }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ctx.createTimer(Duration.ofSeconds(3)).get();
                    ctx.complete(ctx.getInput(Object.class));
                };
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "TwoTimerReplayTester"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ArrayList<Boolean> list = new ArrayList<>();
                    list.add(ctx.getIsReplaying());
                    ctx.createTimer(Duration.ofSeconds(0))
                        .thenRun(() -> list.add(ctx.getIsReplaying()))
                        .thenCompose(Void -> ctx.createTimer(Duration.ofSeconds(0)))
                        .thenRun(() -> {
                            list.add(ctx.getIsReplaying());
                            ctx.complete(list);
                        });
                };
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "OrchestrationWithActivity"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String name = ctx.getInput(String.class);
                    Task<String> task = ctx.callActivity("SayHello", name, String.class);
                    task.thenAccept(ctx::complete);
                };
            }
        });
        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "SayHello"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String name = ctx.getInput(String.class);
                    return String.format("Hello, %s!", name);
                };
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "CurrentDateTimeUtc"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    Instant instant1 = ctx.getCurrentInstant();
                    Task<Instant> t1 = ctx.callActivity("Echo", instant1, Instant.class);
                    t1.thenAccept(result1 -> {
                        if (!result1.equals(instant1)) {
                            ctx.complete(false);
                            return;
                        }

                        Instant instant2 = ctx.getCurrentInstant();
                        Task<Instant> t2 = ctx.callActivity("Echo", instant2, Instant.class);
                        t2.thenAccept(result2 -> {
                            boolean success = result2.equals(instant2);
                            ctx.complete(success);
                        });
                    });
                };
            }
        });
        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "CurrentDateTimeUtc2"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    Instant instant1 = ctx.getCurrentInstant();
                    Instant result1 = ctx.callActivity("Echo", instant1, Instant.class).get();
                    if (!result1.equals(instant1)) {
                        ctx.complete(false);
                        return;
                    }

                    Instant instant2 = ctx.getCurrentInstant();
                    Instant result2 = ctx.callActivity("Echo", instant2, Instant.class).get();

                    boolean success = result2.equals(instant2);
                    ctx.complete(success);
                };
            }
        });
        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Echo"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(Object.class);
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "OrchestrationsWithActivityChain"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // Java requires us to wrap value in an AtomicReference<T> in order
                    // for it to be mutated by a lambda function.
                    AtomicReference<Integer> value = new AtomicReference<>(0);

                    // Each iteration of the for loop appends a new callback stage to the sequence
                    Task<Void> task = ctx.completedTask(null);
                    for (int i = 0; i < 10; i++) {
                        task = task.thenCompose(Void -> ctx
                                .callActivity("PlusOne", value.get(), Integer.class)
                                .thenAccept(value::set));
                    }

                    task.thenRun(() -> ctx.complete(value.get()));
                };
            }
        });
        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "OrchestrationsWithActivityChain2"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    int value = 0;
                    for (int i = 0; i < 10; i++) {
                        value = ctx.callActivity("PlusOne", value, int.class).get();
                    }

                    ctx.complete(value);
                };
            }
        });

        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ActivityFanOut"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // Schedule each task to run in parallel
                    List<Task<String>> parallelTasks = IntStream.range(0, 10)
                            .mapToObj(i -> ctx.callActivity("ToString", i, String.class))
                            .collect(Collectors.toList());

                    // Wait for all tasks to complete, then sort and reverse the results
                    ctx.allOf(parallelTasks).thenAccept(results -> {
                        Collections.sort(results);
                        Collections.reverse(results);
                        ctx.complete(results);
                    });
                };
            }
        });
        server.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ActivityFanOut2"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // Schedule each task to run in parallel
                    List<Task<String>> parallelTasks = IntStream.range(0, 10)
                            .mapToObj(i -> ctx.callActivity("ToString", i, String.class))
                            .collect(Collectors.toList());

                    // Wait for all tasks to complete, then sort and reverse the results
                    List<String> results = ctx.allOf(parallelTasks).get();
                    Collections.sort(results);
                    Collections.reverse(results);
                    ctx.complete(results);
                };
            }
        });
        server.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ToString"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(Object.class).toString();
            }
        });

        server.start();
        server.blockUntilShutdown();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int port;
        private DataConverter dataConverter;

        private Builder() {
        }

        public void setPort(int port) {
            this.port = port;
        }

        public void setDataConverter(DataConverter dataConverter) {
            this.dataConverter = dataConverter;
        }

        public TaskHubServer build() {
            return new TaskHubServer(this);
        }
    }

    class TaskHubWorkerServiceImpl extends TaskHubWorkerServiceImplBase {

        private final TaskOrchestrationExecutor taskOrchestrationExecutor;
        private final TaskActivityExecutor taskActivityExecutor;

        public TaskHubWorkerServiceImpl() {
            this.taskOrchestrationExecutor = new TaskOrchestrationExecutor(
                    TaskHubServer.this.orchestrationFactories,
                    TaskHubServer.this.dataConverter,
                    TaskHubServer.logger);
            this.taskActivityExecutor = new TaskActivityExecutor(
                    TaskHubServer.this.activityFactories,
                    TaskHubServer.this.dataConverter,
                    TaskHubServer.logger);
        }

        @Override
        public void executeOrchestrator(OrchestratorRequest req, StreamObserver<OrchestratorResponse> responses) {
            // TODO: Error handling for when the orchestrator isn't registered
            Collection<OrchestratorAction> actions = this.taskOrchestrationExecutor.execute(
                    req.getPastEventsList(),
                    req.getNewEventsList());
            OrchestratorResponse response = OrchestratorResponse.newBuilder()
                    .addAllActions(actions)
                    .build();
            responses.onNext(response);
            responses.onCompleted();
        }

        @Override
        public void executeActivity(ActivityRequest request, StreamObserver<ActivityResponse> responses) {
            // TODO: Error handling for when the activity isn't registered
            String activityName = request.getName();
            String activityInput = request.getInput().getValue();
            String result = this.taskActivityExecutor.execute(activityName, activityInput);
            ActivityResponse response = ActivityResponse.newBuilder()
                    .setResult(StringValue.of(result))
                    .build();
            responses.onNext(response);
            responses.onCompleted();
        }
    }
}