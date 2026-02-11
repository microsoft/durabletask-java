// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;

import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.WorkItem.RequestCase;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc.*;
import com.microsoft.durabletask.util.VersionUtils;

import io.grpc.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Task hub worker that connects to a sidecar process over gRPC to execute orchestrator and activity events.
 */
public final class DurableTaskGrpcWorker implements AutoCloseable {
    private static final int DEFAULT_PORT = 4001;
    private static final Logger logger = Logger.getLogger(DurableTaskGrpcWorker.class.getPackage().getName());
    private static final Duration DEFAULT_MAXIMUM_TIMER_INTERVAL = Duration.ofDays(3);

    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    private final HashMap<String, TaskEntityFactory> entityFactories = new HashMap<>();

    private final ManagedChannel managedSidecarChannel;
    private final DataConverter dataConverter;
    private final Duration maximumTimerInterval;
    private final DurableTaskGrpcWorkerVersioningOptions versioningOptions;

    private final TaskHubSidecarServiceBlockingStub sidecarClient;

    DurableTaskGrpcWorker(DurableTaskGrpcWorkerBuilder builder) {
        this.orchestrationFactories.putAll(builder.orchestrationFactories);
        this.activityFactories.putAll(builder.activityFactories);
        this.entityFactories.putAll(builder.entityFactories);

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
        this.versioningOptions = builder.versioningOptions;
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
                logger,
                this.versioningOptions);
        TaskActivityExecutor taskActivityExecutor = new TaskActivityExecutor(
                this.activityFactories,
                this.dataConverter,
                logger);
        TaskEntityExecutor taskEntityExecutor = new TaskEntityExecutor(
                this.entityFactories,
                this.dataConverter,
                logger);

        // TODO: How do we interrupt manually?
        while (true) {
            try {
                GetWorkItemsRequest.Builder requestBuilder = GetWorkItemsRequest.newBuilder();
                if (!this.entityFactories.isEmpty()) {
                    // Signal to the sidecar that this worker can handle entity work items
                    requestBuilder.setMaxConcurrentEntityWorkItems(1);
                }
                GetWorkItemsRequest getWorkItemsRequest = requestBuilder.build();
                Iterator<WorkItem> workItemStream = this.sidecarClient.getWorkItems(getWorkItemsRequest);
                while (workItemStream.hasNext()) {
                    WorkItem workItem = workItemStream.next();
                    RequestCase requestType = workItem.getRequestCase();
                    if (requestType == RequestCase.ORCHESTRATORREQUEST) {
                        OrchestratorRequest orchestratorRequest = workItem.getOrchestratorRequest();

                        // If versioning is set, process it first to see if the orchestration should be executed.
                        boolean versioningFailed = false;
                        if (versioningOptions != null && versioningOptions.getVersion() != null) {
                            String version = Stream.concat(orchestratorRequest.getPastEventsList().stream(), orchestratorRequest.getNewEventsList().stream())
                                .filter(event -> event.getEventTypeCase() == HistoryEvent.EventTypeCase.EXECUTIONSTARTED)
                                .map(event -> event.getExecutionStarted().getVersion().getValue())
                                .findFirst()
                                .orElse(null);

                            if (version != null) {
                                int comparison = VersionUtils.compareVersions(version, versioningOptions.getVersion());

                                switch (versioningOptions.getMatchStrategy()) {
                                    case NONE:
                                        break;
                                    case STRICT:
                                        if (comparison != 0) {
                                            logger.log(Level.WARNING, String.format("The orchestration version '%s' does not match the worker version '%s'.", version, versioningOptions.getVersion()));
                                            versioningFailed = true;
                                        }
                                        break;
                                    case CURRENTOROLDER:
                                        if (comparison > 0) {
                                            logger.log(Level.WARNING, String.format("The orchestration version '%s' is greater than the worker version '%s'.", version, versioningOptions.getVersion()));
                                            versioningFailed = true;
                                        }
                                        break;
                                    default:
                                        logger.log(Level.SEVERE, String.format("Unknown version match strategy '%s'.", versioningOptions.getMatchStrategy()));
                                        versioningFailed = true;
                                        break;
                                }
                            }
                        }

                        // TODO: Run this on a worker pool thread: https://www.baeldung.com/thread-pool-java-and-guava
                        // TODO: Error handling
                        if (!versioningFailed) {
                            TaskOrchestratorResult taskOrchestratorResult = taskOrchestrationExecutor.execute(
                                orchestratorRequest.getPastEventsList(),
                                orchestratorRequest.getNewEventsList());

                            OrchestratorResponse response = OrchestratorResponse.newBuilder()
                                    .setInstanceId(orchestratorRequest.getInstanceId())
                                    .addAllActions(taskOrchestratorResult.getActions())
                                    .setCustomStatus(StringValue.of(taskOrchestratorResult.getCustomStatus()))
                                    .setCompletionToken(workItem.getCompletionToken())
                                    .build();

                            this.sidecarClient.completeOrchestratorTask(response);
                        } else {
                            switch(versioningOptions.getFailureStrategy()) {
                                case FAIL:
                                    CompleteOrchestrationAction completeAction = CompleteOrchestrationAction.newBuilder()
                                        .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED)
                                        .setFailureDetails(TaskFailureDetails.newBuilder()
                                            .setErrorType("VersionMismatch")
                                            .setErrorMessage("The orchestration version does not match the worker version.")
                                            .build())
                                        .build();

                                    OrchestratorAction action = OrchestratorAction.newBuilder()
                                        .setCompleteOrchestration(completeAction)
                                        .build();

                                    OrchestratorResponse response = OrchestratorResponse.newBuilder()
                                        .setInstanceId(orchestratorRequest.getInstanceId())
                                        .setCompletionToken(workItem.getCompletionToken())
                                        .addActions(action)
                                        .build();

                                    this.sidecarClient.completeOrchestratorTask(response);
                                    break;
                                // Reject and default share the same behavior as it does not change the orchestration to a terminal state.
                                case REJECT:
                                default:
                                    this.sidecarClient.abandonTaskOrchestratorWorkItem(AbandonOrchestrationTaskRequest.newBuilder()
                                        .setCompletionToken(workItem.getCompletionToken())
                                        .build());
                            }
                        }                        
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
                    } else if (requestType == RequestCase.ENTITYREQUEST) {
                        EntityBatchRequest entityRequest = workItem.getEntityRequest();
                        try {
                            EntityBatchResult result = taskEntityExecutor.execute(entityRequest);
                            EntityBatchResult responseWithToken = result.toBuilder()
                                    .setCompletionToken(workItem.getCompletionToken())
                                    .build();
                            this.sidecarClient.completeEntityTask(responseWithToken);
                        } catch (Exception e) {
                            logger.log(Level.WARNING,
                                    String.format("Failed to execute entity batch for '%s'. Abandoning work item.",
                                            entityRequest.getInstanceId()),
                                    e);
                            this.sidecarClient.abandonTaskEntityWorkItem(
                                    AbandonEntityTaskRequest.newBuilder()
                                            .setCompletionToken(workItem.getCompletionToken())
                                            .build());
                        }
                    } else if (requestType == RequestCase.ENTITYREQUESTV2) {
                        EntityRequest entityRequestV2 = workItem.getEntityRequestV2();
                        try {
                            // Convert V2 (history-based) format to V1 (flat) format
                            EntityBatchRequest.Builder batchBuilder = EntityBatchRequest.newBuilder()
                                    .setInstanceId(entityRequestV2.getInstanceId());
                            if (entityRequestV2.hasEntityState()) {
                                batchBuilder.setEntityState(entityRequestV2.getEntityState());
                            }

                            List<OperationInfo> operationInfos = new ArrayList<>();
                            for (HistoryEvent event : entityRequestV2.getOperationRequestsList()) {
                                if (event.hasEntityOperationSignaled()) {
                                    EntityOperationSignaledEvent signaled = event.getEntityOperationSignaled();
                                    OperationRequest.Builder opBuilder = OperationRequest.newBuilder()
                                            .setRequestId(signaled.getRequestId())
                                            .setOperation(signaled.getOperation());
                                    if (signaled.hasInput()) {
                                        opBuilder.setInput(signaled.getInput());
                                    }
                                    batchBuilder.addOperations(opBuilder.build());
                                    // Fire-and-forget: no response destination
                                    operationInfos.add(OperationInfo.newBuilder()
                                            .setRequestId(signaled.getRequestId())
                                            .build());
                                } else if (event.hasEntityOperationCalled()) {
                                    EntityOperationCalledEvent called = event.getEntityOperationCalled();
                                    OperationRequest.Builder opBuilder = OperationRequest.newBuilder()
                                            .setRequestId(called.getRequestId())
                                            .setOperation(called.getOperation());
                                    if (called.hasInput()) {
                                        opBuilder.setInput(called.getInput());
                                    }
                                    batchBuilder.addOperations(opBuilder.build());
                                    // Two-way call: include response destination
                                    OperationInfo.Builder infoBuilder = OperationInfo.newBuilder()
                                            .setRequestId(called.getRequestId());
                                    if (called.hasParentInstanceId()) {
                                        OrchestrationInstance.Builder destBuilder = OrchestrationInstance.newBuilder()
                                                .setInstanceId(called.getParentInstanceId().getValue());
                                        if (called.hasParentExecutionId()) {
                                            destBuilder.setExecutionId(StringValue.of(called.getParentExecutionId().getValue()));
                                        }
                                        infoBuilder.setResponseDestination(destBuilder.build());
                                    }
                                    operationInfos.add(infoBuilder.build());
                                } else {
                                    logger.log(Level.WARNING,
                                            "Skipping unsupported history event type in ENTITYREQUESTV2: {0}",
                                            event.getEventTypeCase());
                                }
                            }

                            EntityBatchRequest batchRequest = batchBuilder.build();
                            EntityBatchResult result = taskEntityExecutor.execute(batchRequest);

                            // Attach completion token and operation infos for response routing
                            EntityBatchResult.Builder responseBuilder = result.toBuilder()
                                    .setCompletionToken(workItem.getCompletionToken());
                            // Trim operationInfos to match actual result count
                            int resultCount = result.getResultsCount();
                            if (operationInfos.size() > resultCount) {
                                responseBuilder.addAllOperationInfos(operationInfos.subList(0, resultCount));
                            } else {
                                responseBuilder.addAllOperationInfos(operationInfos);
                            }
                            this.sidecarClient.completeEntityTask(responseBuilder.build());
                        } catch (Exception e) {
                            logger.log(Level.WARNING,
                                    String.format("Failed to execute V2 entity batch for '%s'. Abandoning work item.",
                                            entityRequestV2.getInstanceId()),
                                    e);
                            this.sidecarClient.abandonTaskEntityWorkItem(
                                    AbandonEntityTaskRequest.newBuilder()
                                            .setCompletionToken(workItem.getCompletionToken())
                                            .build());
                        }
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
}