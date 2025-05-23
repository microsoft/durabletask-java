// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;

import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.WorkerCapability;
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
    private static final Duration DEFAULT_MAXIMUM_TIMER_INTERVAL = Duration.ofDays(3);

    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();

    private final ManagedChannel managedSidecarChannel;
    private final DataConverter dataConverter;
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
                GetWorkItemsRequest getWorkItemsRequest = GetWorkItemsRequest.newBuilder()
                        .addCapabilities(WorkerCapability.WORKER_CAPABILITY_HISTORY_STREAMING)
                        .build();
                Iterator<WorkItem> workItemStream = this.sidecarClient.getWorkItems(getWorkItemsRequest);
                while (workItemStream.hasNext()) {
                    WorkItem workItem = workItemStream.next();
                    RequestCase requestType = workItem.getRequestCase();
                    if (requestType == RequestCase.ORCHESTRATORREQUEST) {
                        OrchestratorRequest orchestratorRequest = workItem.getOrchestratorRequest();

                        // TODO: Run this on a worker pool thread: https://www.baeldung.com/thread-pool-java-and-guava
                        // TODO: Error handling
                        TaskOrchestratorResult taskOrchestratorResult;
                        
                        if (orchestratorRequest.getRequiresHistoryStreaming()) {
                            // Stream the history events when requested by the orchestrator service
                            taskOrchestratorResult = processOrchestrationWithStreamingHistory(
                                    taskOrchestrationExecutor, 
                                    orchestratorRequest);
                        } else {
                            // Standard non-streaming execution path
                            taskOrchestratorResult = taskOrchestrationExecutor.execute(
                                    orchestratorRequest.getPastEventsList(),
                                    orchestratorRequest.getNewEventsList());
                        }

                        OrchestratorResponse response = OrchestratorResponse.newBuilder()
                                .setInstanceId(orchestratorRequest.getInstanceId())
                                .addAllActions(taskOrchestratorResult.getActions())
                                .setCustomStatus(StringValue.of(taskOrchestratorResult.getCustomStatus()))
                                .setCompletionToken(workItem.getCompletionToken())
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
                                .setTaskId(activityRequest.getTaskId())
                                .setCompletionToken(workItem.getCompletionToken());

                        if (output != null) {
                            responseBuilder.setResult(StringValue.of(output));
                        }

                        if (failureDetails != null) {
                            responseBuilder.setFailureDetails(failureDetails);
                        }

                        this.sidecarClient.completeActivityTask(responseBuilder.build());
                    } 
                    else if (requestType == RequestCase.HEALTHPING)
                    {
                        // No-op
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
                    logger.log(Level.WARNING, String.format("Unexpected failure connecting to %s", this.getSidecarAddress()), e);
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

    /**
     * Process an orchestration request using streaming history instead of receiving the full history in the work item.
     * This is used when the history is too large to fit in a single gRPC message.
     *
     * @param taskOrchestrationExecutor the executor to use for processing the orchestration
     * @param orchestratorRequest the request containing orchestration details
     * @return the result of executing the orchestration
     */
    private TaskOrchestratorResult processOrchestrationWithStreamingHistory(
            TaskOrchestrationExecutor taskOrchestrationExecutor,
            OrchestratorRequest orchestratorRequest) {
        
        logger.fine(() -> String.format(
                "Streaming history for instance '%s' as it requires history streaming",
                orchestratorRequest.getInstanceId()));

        // Create a request to stream the instance history
        StreamInstanceHistoryRequest.Builder requestBuilder = StreamInstanceHistoryRequest.newBuilder()
                .setInstanceId(orchestratorRequest.getInstanceId())
                .setForWorkItemProcessing(true);
                
        // Include execution ID if present
        if (orchestratorRequest.hasExecutionId()) {
            requestBuilder.setExecutionId(orchestratorRequest.getExecutionId());
        }
        
        StreamInstanceHistoryRequest request = requestBuilder.build();

        // Stream history from the service
        List<HistoryEvent> pastEvents = new ArrayList<>();
        List<HistoryEvent> newEvents = new ArrayList<>();
        
        try {
            // Get a stream of history chunks
            Iterator<HistoryChunk> historyStream = this.sidecarClient.streamInstanceHistory(request);
            
            // Process each chunk of history events
            while (historyStream.hasNext()) {
                HistoryChunk chunk = historyStream.next();
                
                // The first chunk is considered the "past events", and the rest are "new events"
                if (pastEvents.isEmpty()) {
                    pastEvents.addAll(chunk.getEventsList());
                } else {
                    newEvents.addAll(chunk.getEventsList());
                }
            }
            
            logger.fine(() -> String.format(
                    "Successfully streamed history for instance '%s': %d past events, %d new events",
                    orchestratorRequest.getInstanceId(), pastEvents.size(), newEvents.size()));
            
            // Execute the orchestration with the collected history events
            return taskOrchestrationExecutor.execute(pastEvents, newEvents);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                logger.log(Level.WARNING, "The sidecar service is unavailable while streaming history for instance " + 
                        orchestratorRequest.getInstanceId());
            } else if (e.getStatus().getCode() == Status.Code.CANCELLED) {
                logger.log(Level.WARNING, "History streaming was canceled for instance " +
                        orchestratorRequest.getInstanceId());
            } else {
                logger.log(Level.WARNING, "Error streaming history for instance " + 
                        orchestratorRequest.getInstanceId(), e);
            }
            throw e;
        }
    }
}