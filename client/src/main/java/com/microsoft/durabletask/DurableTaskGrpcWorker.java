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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Task hub worker that connects to a sidecar process over gRPC to execute orchestrator and activity events.
 */
public final class DurableTaskGrpcWorker implements AutoCloseable {
    private static final int DEFAULT_PORT = 4001;
    static final int DEFAULT_MAX_WORK_ITEM_THREADS = 100;
    private static final Logger logger = Logger.getLogger(DurableTaskGrpcWorker.class.getPackage().getName());
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);
    private static final Duration DEFAULT_MAXIMUM_TIMER_INTERVAL = Duration.ofDays(3);

    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    private final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    private final HashMap<String, TaskEntityFactory> entityFactories = new HashMap<>();

    private final ManagedChannel managedSidecarChannel;
    private final DataConverter dataConverter;
    private final Duration maximumTimerInterval;
    private final DurableTaskGrpcWorkerVersioningOptions versioningOptions;
    private final int maxConcurrentEntityWorkItems;
    private final int maxConcurrentActivityWorkItems;
    private final ExecutorService workItemExecutor;

    private final TaskHubSidecarServiceBlockingStub sidecarClient;

    DurableTaskGrpcWorker(DurableTaskGrpcWorkerBuilder builder) {
        this.orchestrationFactories.putAll(builder.orchestrationFactories);
        this.activityFactories.putAll(builder.activityFactories);
        this.entityFactories.putAll(builder.entityFactories);
        this.maxConcurrentEntityWorkItems = builder.maxConcurrentEntityWorkItems;
        this.maxConcurrentActivityWorkItems = builder.maxConcurrentActivityWorkItems;

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
        int maxThreads = builder.maxWorkItemThreads > 0 ? builder.maxWorkItemThreads : DEFAULT_MAX_WORK_ITEM_THREADS;
        this.workItemExecutor = new ThreadPoolExecutor(
                0, maxThreads,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                r -> {
                    Thread t = new Thread(r, "durabletask-worker-" + THREAD_COUNTER.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
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
        this.workItemExecutor.shutdown();
        try {
            if (!this.workItemExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                this.workItemExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            this.workItemExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (this.managedSidecarChannel != null) {
            try {
                this.managedSidecarChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
                this.versioningOptions,
                true);
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
                if (this.maxConcurrentActivityWorkItems > 0) {
                    requestBuilder.setMaxConcurrentActivityWorkItems(this.maxConcurrentActivityWorkItems);
                }
                if (!this.entityFactories.isEmpty()) {
                    // Signal to the sidecar that this worker can handle entity work items
                    requestBuilder.setMaxConcurrentEntityWorkItems(this.maxConcurrentEntityWorkItems);
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
                            // Extract ExecutionStartedEvent and its timestamp for trace context
                            HistoryEvent startedHistoryEvent = Stream.concat(
                                    orchestratorRequest.getPastEventsList().stream(),
                                    orchestratorRequest.getNewEventsList().stream())
                                .filter(event -> event.getEventTypeCase() == HistoryEvent.EventTypeCase.EXECUTIONSTARTED)
                                .findFirst()
                                .orElse(null);

                            ExecutionStartedEvent startedEvent = startedHistoryEvent != null
                                    ? startedHistoryEvent.getExecutionStarted() : null;

                            TraceContext orchTraceCtx = (startedEvent != null && startedEvent.hasParentTraceContext())
                                    ? startedEvent.getParentTraceContext() : null;
                            String orchName = startedEvent != null ? startedEvent.getName() : "";

                            // Start the orchestration span BEFORE execution so child spans
                            // (activities, timers) are nested under it. Use setSpanId() to give
                            // all dispatches the same span ID for deduplication
                            // (matching .NET's SetSpanId pattern).
                            Span orchestrationSpan = null;
                            TraceContext orchestrationSpanContext = null;
                            if (orchTraceCtx != null) {
                                Map<String, String> orchSpanAttrs = new HashMap<>();
                                orchSpanAttrs.put(TracingHelper.ATTR_TYPE, TracingHelper.TYPE_ORCHESTRATION);
                                orchSpanAttrs.put(TracingHelper.ATTR_TASK_NAME, orchName);
                                orchSpanAttrs.put(TracingHelper.ATTR_INSTANCE_ID, orchestratorRequest.getInstanceId());

                                // Use ExecutionStartedEvent timestamp so the orchestration span
                                // covers the full lifecycle from creation to completion
                                Instant spanStartTime = startedHistoryEvent.hasTimestamp()
                                        ? DataConverter.getInstantFromTimestamp(startedHistoryEvent.getTimestamp())
                                        : null;

                                orchestrationSpan = TracingHelper.startSpanWithStartTime(
                                        TracingHelper.TYPE_ORCHESTRATION + ":" + orchName,
                                        orchTraceCtx,
                                        SpanKind.SERVER,
                                        orchSpanAttrs,
                                        spanStartTime);

                                // Use the same span ID across dispatches for deduplication.
                                // Priority: OrchestrationTraceContext.spanID (if populated by server),
                                // fallback: derive deterministically from parentTraceContext span ID.
                                String orchSpanId = null;
                                if (orchestratorRequest.hasOrchestrationTraceContext()
                                        && orchestratorRequest.getOrchestrationTraceContext().hasSpanID()
                                        && orchestratorRequest.getOrchestrationTraceContext().getSpanID().getValue() != null
                                        && !orchestratorRequest.getOrchestrationTraceContext().getSpanID().getValue().isEmpty()) {
                                    orchSpanId = orchestratorRequest.getOrchestrationTraceContext().getSpanID().getValue();
                                } else {
                                    // Derive from parent span ID by hashing with instance ID
                                    String parentSpanId = TracingHelper.extractSpanIdFromTraceparent(
                                            orchTraceCtx.getTraceParent());
                                    if (parentSpanId != null) {
                                        long hash = parentSpanId.hashCode() * 31L
                                                + orchestratorRequest.getInstanceId().hashCode();
                                        orchSpanId = String.format("%016x", hash);
                                    }
                                }
                                TracingHelper.setSpanId(orchestrationSpan, orchSpanId);

                                orchestrationSpanContext = TracingHelper.getCurrentTraceContext(orchestrationSpan);
                            }

                            TaskOrchestratorResult taskOrchestratorResult;
                            try {
                                taskOrchestratorResult = taskOrchestrationExecutor.execute(
                                    orchestratorRequest.getPastEventsList(),
                                    orchestratorRequest.getNewEventsList(),
                                    orchestrationSpanContext);
                            } catch (Throwable e) {
                                if (e instanceof Error) {
                                    throw (Error) e;
                                }
                                throw new RuntimeException(e);
                            }

                            // Only end (export) the orchestration span on the completion dispatch.
                            // Non-completion dispatches: span is started for child parenting but
                            // never ended, so it won't be exported by the SpanProcessor.
                            if (orchestrationSpan != null) {
                                boolean isCompleting = taskOrchestratorResult.getActions().stream()
                                    .anyMatch(a -> a.getOrchestratorActionTypeCase() == OrchestratorAction.OrchestratorActionTypeCase.COMPLETEORCHESTRATION
                                            || a.getOrchestratorActionTypeCase() == OrchestratorAction.OrchestratorActionTypeCase.TERMINATEORCHESTRATION);

                                if (isCompleting) {
                                    for (OrchestratorAction action : taskOrchestratorResult.getActions()) {
                                        if (action.getOrchestratorActionTypeCase() == OrchestratorAction.OrchestratorActionTypeCase.COMPLETEORCHESTRATION) {
                                            CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                                            if (complete.getOrchestrationStatus() == OrchestrationStatus.ORCHESTRATION_STATUS_FAILED) {
                                                String errorMsg = complete.hasFailureDetails()
                                                        ? complete.getFailureDetails().getErrorMessage()
                                                        : "Orchestration failed";
                                                orchestrationSpan.setStatus(StatusCode.ERROR, errorMsg);
                                            }
                                            break;
                                        }
                                    }
                                    orchestrationSpan.end();
                                }
                                // Non-completion: intentionally NOT ending the span.
                                // Unended spans are not exported by the SpanProcessor.
                            }

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
                        String activityInstanceId = activityRequest.getOrchestrationInstance().getInstanceId();

                        // Start a tracing span for this activity execution
                        TraceContext activityTraceCtx = activityRequest.hasParentTraceContext()
                                ? activityRequest.getParentTraceContext() : null;
                        Map<String, String> spanAttributes = new HashMap<>();
                        spanAttributes.put(TracingHelper.ATTR_TYPE, TracingHelper.TYPE_ACTIVITY);
                        spanAttributes.put(TracingHelper.ATTR_TASK_NAME, activityRequest.getName());
                        spanAttributes.put(TracingHelper.ATTR_INSTANCE_ID, activityInstanceId);
                        spanAttributes.put(TracingHelper.ATTR_TASK_ID, String.valueOf(activityRequest.getTaskId()));
                        Span activitySpan = TracingHelper.startSpan(
                                TracingHelper.TYPE_ACTIVITY + ":" + activityRequest.getName(),
                                activityTraceCtx,
                                SpanKind.SERVER,
                                spanAttributes);
                        Scope activityScope = activitySpan.makeCurrent();

                        this.workItemExecutor.submit(() -> {
                            String output = null;
                            TaskFailureDetails failureDetails = null;
                            Throwable activityError = null;
                            try {
                                output = taskActivityExecutor.execute(
                                    activityRequest.getName(),
                                    activityRequest.getInput().getValue(),
                                    activityRequest.getTaskId());
                            } catch (Throwable e) {
                                activityError = e;
                                failureDetails = TaskFailureDetails.newBuilder()
                                    .setErrorType(e.getClass().getName())
                                    .setErrorMessage(e.getMessage())
                                    .setStackTrace(StringValue.of(FailureDetails.getFullStackTrace(e)))
                                    .build();
                            } finally {
                                activityScope.close();
                                TracingHelper.endSpan(activitySpan, activityError);
                            }

                            try {
                                ActivityResponse.Builder responseBuilder = ActivityResponse.newBuilder()
                                        .setInstanceId(activityInstanceId)
                                        .setTaskId(activityRequest.getTaskId())
                                        .setCompletionToken(workItem.getCompletionToken());

                                if (output != null) {
                                    responseBuilder.setResult(StringValue.of(output));
                                }

                                if (failureDetails != null) {
                                    responseBuilder.setFailureDetails(failureDetails);
                                }

                                this.sidecarClient.completeActivityTask(responseBuilder.build());
                            } catch (Exception e) {
                                logger.log(Level.WARNING,
                                        String.format("Failed to complete activity '%s' for instance '%s'. Abandoning work item.",
                                                activityRequest.getName(),
                                                activityInstanceId),
                                        e);
                                this.sidecarClient.abandonTaskActivityWorkItem(
                                        AbandonActivityTaskRequest.newBuilder()
                                                .setCompletionToken(workItem.getCompletionToken())
                                                .build());
                            }
                        });
                    } else if (requestType == RequestCase.ENTITYREQUEST) {
                        EntityBatchRequest entityRequest = workItem.getEntityRequest();
                        this.workItemExecutor.submit(() -> {
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
                        });
                    } else if (requestType == RequestCase.ENTITYREQUESTV2) {
                        EntityRequest entityRequestV2 = workItem.getEntityRequestV2();
                        this.workItemExecutor.submit(() -> {
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
                                if (operationInfos.size() != resultCount) {
                                    logger.log(Level.WARNING,
                                            String.format("Entity '%s': operationInfos size (%d) does not match result count (%d). Trimming.",
                                                    entityRequestV2.getInstanceId(),
                                                    operationInfos.size(),
                                                    resultCount));
                                }
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
                        });
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
