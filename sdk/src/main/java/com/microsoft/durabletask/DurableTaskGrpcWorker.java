// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;

import com.microsoft.durabletask.protobuf.TaskHubSidecarServiceGrpc;
import com.microsoft.durabletask.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.protobuf.OrchestratorService.WorkItem.RequestCase;
import com.microsoft.durabletask.protobuf.TaskHubSidecarServiceGrpc.*;

import io.grpc.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DurableTaskGrpcWorker implements AutoCloseable {
    private static final int DEFAULT_PORT = 4001;
    private static final Logger logger = Logger.getLogger(DurableTaskGrpcWorker.class.getPackage().getName());

    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();

    private final ManagedChannel managedSidecarChannel;
    private final DataConverter dataConverter;

    private final TaskHubSidecarServiceBlockingStub sidecarClient;

    private DurableTaskGrpcWorker(Builder builder) {
        this.orchestrationFactories.putAll(builder.orchestrationFactories);
        this.activityFactories.putAll(builder.activityFactories);

        Channel sidecarGrpcChannel;
        if (builder.channel != null) {
            // The caller is responsible for managing the channel lifetime
            this.managedSidecarChannel = null;
            sidecarGrpcChannel = builder.channel;
        } else {
            // Construct our own channel using localhost + a port number
            int port = DEFAULT_PORT;
            if (builder.port > 0) {
                port = builder.port;
            }

            // Need to keep track of this channel so we can dispose it on close()
            this.managedSidecarChannel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", port)
                    .usePlaintext()
                    .build();
            sidecarGrpcChannel = this.managedSidecarChannel;
        }

        this.sidecarClient = TaskHubSidecarServiceGrpc.newBlockingStub(sidecarGrpcChannel);
        this.dataConverter = Objects.requireNonNullElse(builder.dataConverter, new JacksonDataConverter());
    }

    public void start() {
        new Thread(() -> {
            try {
                this.runAndBlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void close() {
        if (this.managedSidecarChannel != null) {
            try {
                this.managedSidecarChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Best effort. Also note that AutoClose documentation recommends NOT having
                // close() methods throw InterruptedException:
                // https://docs.oracle.com/javase/7/docs/api/java/lang/AutoCloseable.html
            }
        }
    }

    private String getSidecarAddress() {
        return this.sidecarClient.getChannel().authority();
    }

    public void runAndBlock() throws InterruptedException {
        logger.log(Level.INFO, "Durable Task worker is connecting to sidecar at {0}.", this.getSidecarAddress());

        var taskOrchestrationExecutor = new TaskOrchestrationExecutor(
                this.orchestrationFactories,
                this.dataConverter,
                logger);
        var taskActivityExecutor = new TaskActivityExecutor(
                this.activityFactories,
                this.dataConverter,
                logger);

        // TODO: How do we interrupt manually?
        while (true) {
            try {
                GetWorkItemsRequest getWorkItemsRequest = GetWorkItemsRequest.newBuilder().build();
                Iterator<WorkItem> workItemStream = this.sidecarClient.getWorkItems(getWorkItemsRequest);
                while (workItemStream.hasNext()) {
                    WorkItem workItem = workItemStream.next();
                    RequestCase requestType = workItem.getRequestCase();
                    if (requestType == RequestCase.ORCHESTRATORREQUEST) {
                        OrchestratorRequest orchestratorRequest = workItem.getOrchestratorRequest();

                        // TODO: Run this on a worker pool thread: https://www.baeldung.com/thread-pool-java-and-guava
                        // TODO: Error handling
                        Collection<OrchestratorAction> actions = taskOrchestrationExecutor.execute(
                                orchestratorRequest.getPastEventsList(),
                                orchestratorRequest.getNewEventsList());

                        // TODO: Need to get custom status from executor
                        OrchestratorResponse response = OrchestratorResponse.newBuilder()
                                .setInstanceId(orchestratorRequest.getInstanceId())
                                .addAllActions(actions)
                                .build();

                        this.sidecarClient.completeOrchestratorTask(response);
                    } else if (requestType == RequestCase.ACTIVITYREQUEST) {
                        ActivityRequest activityRequest = workItem.getActivityRequest();

                        // TODO: Run this on a worker pool thread: https://www.baeldung.com/thread-pool-java-and-guava
                        String output = null;
                        TaskFailureDetails failureDetails = null;
                        try {
                            output = taskActivityExecutor.execute(
                                activityRequest.getName(),
                                activityRequest.getInput().getValue(),
                                activityRequest.getTaskId());
                        } catch (Throwable e) {
                            failureDetails = TaskFailureDetails.newBuilder()
                                .setErrorName(e.getClass().getName())
                                .setErrorMessage(e.getMessage())
                                .setErrorDetails(ErrorDetails.getFullStackTrace(e))
                                .build();
                        }

                        ActivityResponse.Builder responseBuilder = ActivityResponse.newBuilder()
                                .setInstanceId(activityRequest.getOrchestrationInstance().getInstanceId())
                                .setTaskId(activityRequest.getTaskId());

                        if (output != null) {
                            responseBuilder.setResult(StringValue.of(output));
                        }

                        if (failureDetails != null) {
                            responseBuilder.setFailureDetails(failureDetails);
                        }

                        this.sidecarClient.completeActivityTask(responseBuilder.build());
                    } else {
                        logger.log(Level.WARNING, "Received and dropped an unknown '{0}' work-item from the sidecar.", requestType);
                    }
                }
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                    logger.log(Level.INFO, "The sidecar at address {0} is unavailable. Will continue retrying.", this.getSidecarAddress());
                } else if (e.getStatus().getCode() == Status.Code.CANCELLED) {
                    logger.log(Level.INFO, "Durable Task worker has disconnected from {0}.", this.getSidecarAddress()); 
                } else {
                    logger.log(Level.WARNING, "Unexpected failure connecting to {0}.", this.getSidecarAddress());
                }

                // Retry after 5 seconds
                Thread.sleep(5000);
            }
        }
    }

    public void stop() {
        this.close();
    }

    /**
     * Main launches the worker from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        DurableTaskGrpcWorker.Builder builder = DurableTaskGrpcWorker.newBuilder();
        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "PlusOne"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(int.class) + 1;
            }
        });

        builder.addOrchestration(new TaskOrchestrationFactory() {
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

        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addOrchestration(new TaskOrchestrationFactory() {
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

        builder.addOrchestration(new TaskOrchestrationFactory() {
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

        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addActivity(new TaskActivityFactory() {
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

        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "Echo"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(Object.class);
            }
        });

        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addOrchestration(new TaskOrchestrationFactory() {
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

        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addOrchestration(new TaskOrchestrationFactory() {
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
        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ToString"; }

            @Override
            public TaskActivity create() {
                return ctx -> ctx.getInput(Object.class).toString();
            }
        });

        final DurableTaskGrpcWorker server = builder.build();
        server.runAndBlock();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
        private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
        private int port;
        private Channel channel;
        private DataConverter dataConverter;

        private Builder() {
        }

        public Builder addOrchestration(TaskOrchestrationFactory factory) {
            String key = factory.getName();
            if (key == null || key.length() == 0) {
                throw new IllegalArgumentException("A non-empty task orchestration name is required.");
            }

            if (this.orchestrationFactories.containsKey(key)) {
                throw new IllegalArgumentException(
                        String.format("A task orchestration factory named %s is already registered.", key));
            }

            this.orchestrationFactories.put(key, factory);
            return this;
        }

        public Builder addActivity(TaskActivityFactory factory) {
            // TODO: Input validation
            String key = factory.getName();
            if (key == null || key.length() == 0) {
                throw new IllegalArgumentException("A non-empty task activity name is required.");
            }

            if (this.activityFactories.containsKey(key)) {
                throw new IllegalArgumentException(
                        String.format("A task activity factory named %s is already registered.", key));
            }

            this.activityFactories.put(key, factory);
            return this;
        }

        public Builder useGrpcChannel(Channel channel) {
            this.channel = channel;
            return this;
        }

        public Builder forPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setDataConverter(DataConverter dataConverter) {
            this.dataConverter = dataConverter;
            return this;
        }

        public DurableTaskGrpcWorker build() {
            return new DurableTaskGrpcWorker(this);
        }
    }

    // class TaskHubWorkerServiceImpl extends TaskHubWorkerServiceImplBase {

    //     private final TaskOrchestrationExecutor taskOrchestrationExecutor;
    //     private final TaskActivityExecutor taskActivityExecutor;
    //     private final DataConverter dataConverter;

    //     public TaskHubWorkerServiceImpl() {
    //         this.dataConverter =  DurableTaskGrpcWorker.this.dataConverter;
    //         this.taskOrchestrationExecutor = new TaskOrchestrationExecutor(
    //                 DurableTaskGrpcWorker.this.orchestrationFactories,
    //                 DurableTaskGrpcWorker.this.dataConverter,
    //                 DurableTaskGrpcWorker.logger);
    //         this.taskActivityExecutor = new TaskActivityExecutor(
    //                 DurableTaskGrpcWorker.this.activityFactories,
    //                 DurableTaskGrpcWorker.this.dataConverter,
    //                 DurableTaskGrpcWorker.logger);
    //     }

    //     @Override
    //     public void executeOrchestrator(OrchestratorRequest req, StreamObserver<OrchestratorResponse> responses) {
    //         // TODO: Error handling for when the orchestrator isn't registered
    //         Collection<OrchestratorAction> actions = this.taskOrchestrationExecutor.execute(
    //                 req.getPastEventsList(),
    //                 req.getNewEventsList());
    //         OrchestratorResponse response = OrchestratorResponse.newBuilder()
    //                 .addAllActions(actions)
    //                 .build();
    //         responses.onNext(response);
    //         responses.onCompleted();
    //     }

    //     @Override
    //     public void executeActivity(ActivityRequest request, StreamObserver<ActivityResponse> responses) {
    //         // TODO: Error handling for when the activity isn't registered
    //         String activityName = request.getName();
    //         String activityInput = request.getInput().getValue();
    //         try {
    //             ActivityResponse.Builder response = ActivityResponse.newBuilder();
    //             String output = this.taskActivityExecutor.execute(activityName, activityInput);
    //             if (output != null) {
    //                 response.setResult(StringValue.of(output));
    //             }
    //             responses.onNext(response.build());
    //             responses.onCompleted();
    //         } catch (Exception ex) {
    //             String details = ErrorDetails.getFullStackTrace(ex);
    //             Status errorStatus = Status.UNKNOWN.withDescription(details);
    //             responses.onError(new StatusException(errorStatus));
    //         }
    //     }
    // }
}