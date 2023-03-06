// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;

import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.WorkItem.RequestCase;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc.*;

import io.grpc.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task hub worker that connects to a sidecar process over gRPC to execute orchestrator and activity events.
 */
public final class DurableTaskGrpcWorker implements AutoCloseable {
    private static final int DEFAULT_PORT = 4001;
    private static final Logger logger = Logger.getLogger(DurableTaskGrpcWorker.class.getPackage().getName());

    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();

    private final ManagedChannel managedSidecarChannel;
    private final DataConverter dataConverter;
    private static final Duration DEFAULT_MAXIMUM_TIMER_INTERVAL = Duration.ofDays(3);
    private final Duration maximumTimerInterval;

    private final TaskHubSidecarServiceBlockingStub sidecarClient;

    DurableTaskGrpcWorker(DurableTaskGrpcWorkerBuilder builder) {
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
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            sidecarGrpcChannel = this.managedSidecarChannel;
        }

        this.sidecarClient = TaskHubSidecarServiceGrpc.newBlockingStub(sidecarGrpcChannel);
        this.dataConverter = builder.dataConverter != null ? builder.dataConverter : new JacksonDataConverter();
        this.maximumTimerInterval = builder.maximumTimerInterval != null ? builder.maximumTimerInterval : DEFAULT_MAXIMUM_TIMER_INTERVAL;
    }

    /**
     * Establishes a gRPC connection to the sidecar and starts processing work-items in the background.
     * <p>
     * This method retries continuously to establish a connection to the sidecar. If a connection fails,
     * a warning log message will be written and a new connection attempt will be made. This process
     * continues until either a connection succeeds or the process receives an interrupt signal.
     */
    public void start() {
        new Thread(this::startAndBlock).start();
    }

    /**
     * Closes the internally managed gRPC channel, if one exists.
     * <p>
     * This method is a no-op if this client object was created using a builder with a gRPC channel object explicitly
     * configured.
     */
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

    /**
     * Establishes a gRPC connection to the sidecar and starts processing work-items on the current thread.
     * This method call blocks indefinitely, or until the current thread is interrupted.
     * <p>
     * Use can alternatively use the {@link #start} method to run orchestration processing in a background thread.
     * <p>
     * This method retries continuously to establish a connection to the sidecar. If a connection fails,
     * a warning log message will be written and a new connection attempt will be made. This process
     * continues until either a connection succeeds or the process receives an interrupt signal.
     */
    public void startAndBlock() {
        logger.log(Level.INFO, "Durable Task worker is connecting to sidecar at {0}.", this.getSidecarAddress());

        TaskOrchestrationExecutor taskOrchestrationExecutor = new TaskOrchestrationExecutor(
                this.orchestrationFactories,
                this.dataConverter,
                this.maximumTimerInterval,
                logger);
        TaskActivityExecutor taskActivityExecutor = new TaskActivityExecutor(
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
                        TaskOrchestratorResult taskOrchestratorResult = taskOrchestrationExecutor.execute(
                                orchestratorRequest.getPastEventsList(),
                                orchestratorRequest.getNewEventsList());

                        OrchestratorResponse response = OrchestratorResponse.newBuilder()
                                .setInstanceId(orchestratorRequest.getInstanceId())
                                .addAllActions(taskOrchestratorResult.getActions())
                                .setCustomStatus(StringValue.of(taskOrchestratorResult.getCustomStatus()))
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
                                .setErrorType(e.getClass().getName())
                                .setErrorMessage(e.getMessage())
                                .setStackTrace(StringValue.of(FailureDetails.getFullStackTrace(e)))
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
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    }

    /**
     * Stops the current worker's listen loop, preventing any new orchestrator or activity events from being processed.
     */
    public void stop() {
        this.close();
    }
}