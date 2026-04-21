// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc.TaskHubSidecarServiceBlockingStub;

import io.grpc.*;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC {@link ClientInterceptor} that externalizes large payloads to a {@link PayloadStore}
 * on outbound requests and resolves payload tokens on inbound responses.
 * <p>
 * This interceptor transparently replaces payload fields that exceed the configured threshold
 * with opaque blob reference tokens, and resolves those tokens back to the original payload
 * on the response path. It works with both unary and server-streaming gRPC calls.
 */
public final class LargePayloadInterceptor implements ClientInterceptor {

    private static final Logger logger = Logger.getLogger(LargePayloadInterceptor.class.getName());

    private final PayloadStore payloadStore;
    private final LargePayloadStorageOptions options;

    /**
     * Creates a new {@code LargePayloadInterceptor}.
     *
     * @param payloadStore the payload store for uploading/downloading payloads
     * @param options the storage options (threshold, max size, etc.)
     */
    public LargePayloadInterceptor(PayloadStore payloadStore, LargePayloadStorageOptions options) {
        if (payloadStore == null) {
            throw new IllegalArgumentException("payloadStore must not be null.");
        }
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }
        if (options.getMaxPayloadBytes() < options.getThresholdBytes()) {
            throw new IllegalArgumentException(
                "maxPayloadBytes (" + options.getMaxPayloadBytes() +
                ") must be >= thresholdBytes (" + options.getThresholdBytes() + ").");
        }
        this.payloadStore = payloadStore;
        this.options = options;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        // Capture the underlying channel so the listener can send poison-pill completions
        // for permanently un-resolvable work items (e.g. blob 404). Using `next` bypasses
        // this interceptor and avoids re-entrancy on the resolve path.
        final Channel underlying = next;

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                Listener<RespT> wrappedListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                        responseListener) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public void onMessage(RespT message) {
                        RespT resolved;
                        try {
                            resolved = (RespT) resolveResponsePayloads(message);
                        } catch (Exception ex) {
                            // If this is a streamed WorkItem and the failure is permanent,
                            // complete the work item as non-retriable failed and drop it from
                            // the stream so the worker is not stuck re-receiving a poison message.
                            if (message instanceof WorkItem
                                    && isPermanentStorageFailure(ex)
                                    && tryFailWorkItem((WorkItem) message, ex, underlying)) {
                                return;
                            }
                            boolean permanent = isPermanentStorageFailure(ex);
                            logger.log(Level.SEVERE,
                                (permanent ? "Permanent" : "Transient")
                                    + " failure resolving externalized payload from blob storage.",
                                ex);
                            // Transient failures use UNAVAILABLE so the caller/sidecar applies
                            // standard backoff semantics. Permanent failures use INTERNAL to
                            // signal that retrying on the current input is unlikely to help.
                            Status status = permanent ? Status.INTERNAL : Status.UNAVAILABLE;
                            throw status
                                .withDescription("Failed to resolve externalized payload: " + ex.getMessage())
                                .withCause(ex)
                                .asRuntimeException();
                        }
                        super.onMessage(resolved);
                    }
                };
                super.start(wrappedListener, headers);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void sendMessage(ReqT message) {
                ReqT externalized = (ReqT) externalizeRequestPayloads(message);
                super.sendMessage(externalized);
            }
        };
    }

    // ==================== Request externalization ====================

    private Object externalizeRequestPayloads(Object request) {
        if (request instanceof CreateInstanceRequest) {
            CreateInstanceRequest r = (CreateInstanceRequest) request;
            StringValue externalized = maybeExternalize(r.getInput());
            if (externalized != r.getInput()) {
                return r.toBuilder().setInput(externalized).build();
            }
        } else if (request instanceof RaiseEventRequest) {
            RaiseEventRequest r = (RaiseEventRequest) request;
            StringValue externalized = maybeExternalize(r.getInput());
            if (externalized != r.getInput()) {
                return r.toBuilder().setInput(externalized).build();
            }
        } else if (request instanceof TerminateRequest) {
            TerminateRequest r = (TerminateRequest) request;
            StringValue externalized = maybeExternalize(r.getOutput());
            if (externalized != r.getOutput()) {
                return r.toBuilder().setOutput(externalized).build();
            }
        } else if (request instanceof SuspendRequest) {
            SuspendRequest r = (SuspendRequest) request;
            StringValue externalized = maybeExternalize(r.getReason());
            if (externalized != r.getReason()) {
                return r.toBuilder().setReason(externalized).build();
            }
        } else if (request instanceof ResumeRequest) {
            ResumeRequest r = (ResumeRequest) request;
            StringValue externalized = maybeExternalize(r.getReason());
            if (externalized != r.getReason()) {
                return r.toBuilder().setReason(externalized).build();
            }
        } else if (request instanceof SignalEntityRequest) {
            SignalEntityRequest r = (SignalEntityRequest) request;
            StringValue externalized = maybeExternalize(r.getInput());
            if (externalized != r.getInput()) {
                return r.toBuilder().setInput(externalized).build();
            }
        } else if (request instanceof ActivityResponse) {
            return externalizeActivityResponse((ActivityResponse) request);
        } else if (request instanceof OrchestratorResponse) {
            return externalizeOrchestratorResponse((OrchestratorResponse) request);
        } else if (request instanceof EntityBatchResult) {
            return externalizeEntityBatchResult((EntityBatchResult) request);
        } else if (request instanceof EntityBatchRequest) {
            return externalizeEntityBatchRequest((EntityBatchRequest) request);
        } else if (request instanceof EntityRequest) {
            EntityRequest r = (EntityRequest) request;
            StringValue externalized = maybeExternalize(r.getEntityState());
            if (externalized != r.getEntityState()) {
                return r.toBuilder().setEntityState(externalized).build();
            }
        }
        return request;
    }

    private Object externalizeActivityResponse(ActivityResponse r) {
        try {
            ActivityResponse.Builder builder = null;
            StringValue externalized = maybeExternalize(r.getResult());
            if (externalized != r.getResult()) {
                builder = r.toBuilder().setResult(externalized);
            }
            // Externalize any user-supplied stack trace on a failed activity response.
            if (r.hasFailureDetails()) {
                TaskFailureDetails fd = externalizeFailureDetails(r.getFailureDetails());
                if (fd != r.getFailureDetails()) {
                    builder = (builder != null ? builder : r.toBuilder()).setFailureDetails(fd);
                }
            }
            if (builder != null) {
                return builder.build();
            }
        } catch (Exception ex) {
            boolean permanent = isPermanentStorageFailure(ex);
            String prefix = permanent
                ? "Permanent payload storage failure"
                : "Transient payload storage failure";
            logger.log(Level.WARNING, prefix + " while externalizing activity response.", ex);
            // Convert to a failure response so the orchestration sees a failed activity.
            // Permanent failures are non-retriable; transient failures are retriable so
            // the sidecar can re-dispatch the work item.
            return r.toBuilder()
                .clearResult()
                .setFailureDetails(TaskFailureDetails.newBuilder()
                    .setErrorType(ex.getClass().getName())
                    .setErrorMessage(prefix + ": " + ex.getMessage())
                    .setStackTrace(StringValue.of(getStackTraceString(ex)))
                    .setIsNonRetriable(permanent)
                    .build())
                .build();
        }
        return r;
    }

    private Object externalizeOrchestratorResponse(OrchestratorResponse r) {
        try {
            OrchestratorResponse.Builder builder = r.toBuilder();
            boolean changed = false;

            StringValue customStatus = maybeExternalize(r.getCustomStatus());
            if (customStatus != r.getCustomStatus()) {
                builder.setCustomStatus(customStatus);
                changed = true;
            }

            for (int i = 0; i < r.getActionsCount(); i++) {
                OrchestratorAction action = r.getActions(i);
                OrchestratorAction externalized = externalizeOrchestratorAction(action);
                if (externalized != action) {
                    builder.setActions(i, externalized);
                    changed = true;
                }
            }

            return changed ? builder.build() : r;
        } catch (Exception ex) {
            boolean permanent = isPermanentStorageFailure(ex);
            String prefix = permanent
                ? "Permanent payload storage failure"
                : "Transient payload storage failure";
            logger.log(Level.WARNING, prefix + " while externalizing orchestrator response.", ex);
            // Replace with a single Failed completion.
            // Permanent failures are non-retriable; transient failures are retriable so
            // the sidecar can re-dispatch the orchestration.
            return OrchestratorResponse.newBuilder()
                .setInstanceId(r.getInstanceId())
                .setCompletionToken(r.getCompletionToken())
                .setIsPartial(false)
                .addActions(OrchestratorAction.newBuilder()
                    .setCompleteOrchestration(CompleteOrchestrationAction.newBuilder()
                        .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED)
                        .setFailureDetails(TaskFailureDetails.newBuilder()
                            .setErrorType(ex.getClass().getName())
                            .setErrorMessage(prefix + ": " + ex.getMessage())
                            .setStackTrace(StringValue.of(getStackTraceString(ex)))
                            .setIsNonRetriable(permanent)
                            .build())
                        .build())
                    .build())
                .build();
        }
    }

    private OrchestratorAction externalizeOrchestratorAction(OrchestratorAction action) {
        OrchestratorAction.Builder builder = null;

        switch (action.getOrchestratorActionTypeCase()) {
            case COMPLETEORCHESTRATION: {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                CompleteOrchestrationAction.Builder cb = null;

                StringValue result = maybeExternalize(complete.getResult());
                if (result != complete.getResult()) {
                    cb = complete.toBuilder().setResult(result);
                }
                StringValue details = maybeExternalize(complete.getDetails());
                if (details != complete.getDetails()) {
                    cb = (cb != null ? cb : complete.toBuilder()).setDetails(details);
                }
                // Externalize any carryover events (populated on continue-as-new with
                // buffered EventRaised / ExecutionStarted payloads for the next instance).
                for (int i = 0; i < complete.getCarryoverEventsCount(); i++) {
                    HistoryEvent carryover = complete.getCarryoverEvents(i);
                    HistoryEvent externalized = externalizeEventPayloads(carryover);
                    if (externalized != carryover) {
                        cb = (cb != null ? cb : complete.toBuilder()).setCarryoverEvents(i, externalized);
                    }
                }
                // Externalize the terminal failure's stack trace (user content, unbounded).
                if (complete.hasFailureDetails()) {
                    TaskFailureDetails fd = externalizeFailureDetails(complete.getFailureDetails());
                    if (fd != complete.getFailureDetails()) {
                        cb = (cb != null ? cb : complete.toBuilder()).setFailureDetails(fd);
                    }
                }
                if (cb != null) {
                    builder = action.toBuilder().setCompleteOrchestration(cb.build());
                }
                break;
            }
            case TERMINATEORCHESTRATION: {
                TerminateOrchestrationAction term = action.getTerminateOrchestration();
                StringValue reason = maybeExternalize(term.getReason());
                if (reason != term.getReason()) {
                    builder = action.toBuilder()
                        .setTerminateOrchestration(term.toBuilder().setReason(reason).build());
                }
                break;
            }
            case SCHEDULETASK: {
                ScheduleTaskAction schedule = action.getScheduleTask();
                StringValue input = maybeExternalize(schedule.getInput());
                if (input != schedule.getInput()) {
                    builder = action.toBuilder()
                        .setScheduleTask(schedule.toBuilder().setInput(input).build());
                }
                break;
            }
            case CREATESUBORCHESTRATION: {
                CreateSubOrchestrationAction sub = action.getCreateSubOrchestration();
                StringValue input = maybeExternalize(sub.getInput());
                if (input != sub.getInput()) {
                    builder = action.toBuilder()
                        .setCreateSubOrchestration(sub.toBuilder().setInput(input).build());
                }
                break;
            }
            case SENDEVENT: {
                SendEventAction sendEvt = action.getSendEvent();
                StringValue data = maybeExternalize(sendEvt.getData());
                if (data != sendEvt.getData()) {
                    builder = action.toBuilder()
                        .setSendEvent(sendEvt.toBuilder().setData(data).build());
                }
                break;
            }
            case SENDENTITYMESSAGE: {
                SendEntityMessageAction entityMsg = action.getSendEntityMessage();
                SendEntityMessageAction.Builder emBuilder = null;

                if (entityMsg.hasEntityOperationSignaled()) {
                    EntityOperationSignaledEvent sig = entityMsg.getEntityOperationSignaled();
                    StringValue input = maybeExternalize(sig.getInput());
                    if (input != sig.getInput()) {
                        emBuilder = entityMsg.toBuilder()
                            .setEntityOperationSignaled(sig.toBuilder().setInput(input).build());
                    }
                } else if (entityMsg.hasEntityOperationCalled()) {
                    EntityOperationCalledEvent called = entityMsg.getEntityOperationCalled();
                    StringValue input = maybeExternalize(called.getInput());
                    if (input != called.getInput()) {
                        emBuilder = entityMsg.toBuilder()
                            .setEntityOperationCalled(called.toBuilder().setInput(input).build());
                    }
                }
                if (emBuilder != null) {
                    builder = action.toBuilder()
                        .setSendEntityMessage(emBuilder.build());
                }
                break;
            }
            default:
                break;
        }

        return builder != null ? builder.build() : action;
    }

    private Object externalizeEntityBatchResult(EntityBatchResult r) {
        EntityBatchResult.Builder builder = r.toBuilder();
        boolean changed = false;

        StringValue entityState = maybeExternalize(r.getEntityState());
        if (entityState != r.getEntityState()) {
            builder.setEntityState(entityState);
            changed = true;
        }

        for (int i = 0; i < r.getResultsCount(); i++) {
            OperationResult result = r.getResults(i);
            if (result.hasSuccess()) {
                OperationResultSuccess success = result.getSuccess();
                StringValue resultVal = maybeExternalize(success.getResult());
                if (resultVal != success.getResult()) {
                    builder.setResults(i, result.toBuilder()
                        .setSuccess(success.toBuilder().setResult(resultVal).build())
                        .build());
                    changed = true;
                }
            }
        }

        for (int i = 0; i < r.getActionsCount(); i++) {
            OperationAction action = r.getActions(i);
            if (action.hasSendSignal()) {
                SendSignalAction sendSig = action.getSendSignal();
                StringValue input = maybeExternalize(sendSig.getInput());
                if (input != sendSig.getInput()) {
                    builder.setActions(i, action.toBuilder()
                        .setSendSignal(sendSig.toBuilder().setInput(input).build())
                        .build());
                    changed = true;
                }
            }
            if (action.hasStartNewOrchestration()) {
                StartNewOrchestrationAction start = action.getStartNewOrchestration();
                StringValue input = maybeExternalize(start.getInput());
                if (input != start.getInput()) {
                    builder.setActions(i, action.toBuilder()
                        .setStartNewOrchestration(start.toBuilder().setInput(input).build())
                        .build());
                    changed = true;
                }
            }
        }

        return changed ? builder.build() : r;
    }

    private Object externalizeEntityBatchRequest(EntityBatchRequest r) {
        EntityBatchRequest.Builder builder = r.toBuilder();
        boolean changed = false;

        StringValue entityState = maybeExternalize(r.getEntityState());
        if (entityState != r.getEntityState()) {
            builder.setEntityState(entityState);
            changed = true;
        }

        for (int i = 0; i < r.getOperationsCount(); i++) {
            OperationRequest op = r.getOperations(i);
            StringValue input = maybeExternalize(op.getInput());
            if (input != op.getInput()) {
                builder.setOperations(i, op.toBuilder().setInput(input).build());
                changed = true;
            }
        }

        return changed ? builder.build() : r;
    }

    // ==================== Response resolution ====================

    private Object resolveResponsePayloads(Object response) {
        if (response instanceof GetInstanceResponse) {
            GetInstanceResponse r = (GetInstanceResponse) response;
            if (r.hasOrchestrationState()) {
                OrchestrationState resolved = resolveOrchestrationState(r.getOrchestrationState());
                if (resolved != r.getOrchestrationState()) {
                    return r.toBuilder().setOrchestrationState(resolved).build();
                }
            }
        } else if (response instanceof QueryInstancesResponse) {
            QueryInstancesResponse r = (QueryInstancesResponse) response;
            QueryInstancesResponse.Builder builder = null;
            for (int i = 0; i < r.getOrchestrationStateCount(); i++) {
                OrchestrationState resolved = resolveOrchestrationState(r.getOrchestrationState(i));
                if (resolved != r.getOrchestrationState(i)) {
                    if (builder == null) builder = r.toBuilder();
                    builder.setOrchestrationState(i, resolved);
                }
            }
            if (builder != null) return builder.build();
        } else if (response instanceof HistoryChunk) {
            HistoryChunk r = (HistoryChunk) response;
            HistoryChunk.Builder builder = null;
            for (int i = 0; i < r.getEventsCount(); i++) {
                HistoryEvent resolved = resolveEventPayloads(r.getEvents(i));
                if (resolved != r.getEvents(i)) {
                    if (builder == null) builder = r.toBuilder();
                    builder.setEvents(i, resolved);
                }
            }
            if (builder != null) return builder.build();
        } else if (response instanceof GetEntityResponse) {
            GetEntityResponse r = (GetEntityResponse) response;
            if (r.hasEntity()) {
                EntityMetadata em = r.getEntity();
                StringValue resolved = maybeResolve(em.getSerializedState());
                if (resolved != em.getSerializedState()) {
                    return r.toBuilder().setEntity(em.toBuilder().setSerializedState(resolved).build()).build();
                }
            }
        } else if (response instanceof QueryEntitiesResponse) {
            QueryEntitiesResponse r = (QueryEntitiesResponse) response;
            QueryEntitiesResponse.Builder builder = null;
            for (int i = 0; i < r.getEntitiesCount(); i++) {
                EntityMetadata em = r.getEntities(i);
                StringValue resolved = maybeResolve(em.getSerializedState());
                if (resolved != em.getSerializedState()) {
                    if (builder == null) builder = r.toBuilder();
                    builder.setEntities(i, em.toBuilder().setSerializedState(resolved).build());
                }
            }
            if (builder != null) return builder.build();
        } else if (response instanceof WorkItem) {
            return resolveWorkItem((WorkItem) response);
        }
        return response;
    }

    private Object resolveWorkItem(WorkItem wi) {
        WorkItem.Builder builder = null;

        // Resolve activity input
        if (wi.hasActivityRequest()) {
            ActivityRequest ar = wi.getActivityRequest();
            StringValue resolved = maybeResolve(ar.getInput());
            if (resolved != ar.getInput()) {
                builder = wi.toBuilder().setActivityRequest(ar.toBuilder().setInput(resolved).build());
            }
        }

        // Resolve orchestration history events
        if (wi.hasOrchestratorRequest()) {
            OrchestratorRequest or = wi.getOrchestratorRequest();
            OrchestratorRequest.Builder orBuilder = null;

            for (int i = 0; i < or.getPastEventsCount(); i++) {
                HistoryEvent resolved = resolveEventPayloads(or.getPastEvents(i));
                if (resolved != or.getPastEvents(i)) {
                    if (orBuilder == null) orBuilder = or.toBuilder();
                    orBuilder.setPastEvents(i, resolved);
                }
            }
            for (int i = 0; i < or.getNewEventsCount(); i++) {
                HistoryEvent resolved = resolveEventPayloads(or.getNewEvents(i));
                if (resolved != or.getNewEvents(i)) {
                    if (orBuilder == null) orBuilder = or.toBuilder();
                    orBuilder.setNewEvents(i, resolved);
                }
            }
            if (orBuilder != null) {
                builder = (builder != null ? builder : wi.toBuilder())
                    .setOrchestratorRequest(orBuilder.build());
            }
        }

        // Resolve entity V1 request
        if (wi.hasEntityRequest()) {
            EntityBatchRequest er = wi.getEntityRequest();
            EntityBatchRequest.Builder erBuilder = null;

            StringValue entityState = maybeResolve(er.getEntityState());
            if (entityState != er.getEntityState()) {
                erBuilder = er.toBuilder().setEntityState(entityState);
            }

            for (int i = 0; i < er.getOperationsCount(); i++) {
                OperationRequest op = er.getOperations(i);
                StringValue input = maybeResolve(op.getInput());
                if (input != op.getInput()) {
                    if (erBuilder == null) erBuilder = er.toBuilder();
                    erBuilder.setOperations(i, op.toBuilder().setInput(input).build());
                }
            }

            if (erBuilder != null) {
                builder = (builder != null ? builder : wi.toBuilder())
                    .setEntityRequest(erBuilder.build());
            }
        }

        // Resolve entity V2 request
        if (wi.hasEntityRequestV2()) {
            EntityRequest er2 = wi.getEntityRequestV2();
            EntityRequest.Builder er2Builder = null;

            StringValue entityState = maybeResolve(er2.getEntityState());
            if (entityState != er2.getEntityState()) {
                er2Builder = er2.toBuilder().setEntityState(entityState);
            }

            for (int i = 0; i < er2.getOperationRequestsCount(); i++) {
                HistoryEvent opEvt = er2.getOperationRequests(i);
                HistoryEvent resolved = resolveEventPayloads(opEvt);
                if (resolved != opEvt) {
                    if (er2Builder == null) er2Builder = er2.toBuilder();
                    er2Builder.setOperationRequests(i, resolved);
                }
            }

            if (er2Builder != null) {
                builder = (builder != null ? builder : wi.toBuilder())
                    .setEntityRequestV2(er2Builder.build());
            }
        }

        return builder != null ? builder.build() : wi;
    }

    private OrchestrationState resolveOrchestrationState(OrchestrationState s) {
        OrchestrationState.Builder builder = null;

        StringValue input = maybeResolve(s.getInput());
        if (input != s.getInput()) {
            builder = s.toBuilder().setInput(input);
        }
        StringValue output = maybeResolve(s.getOutput());
        if (output != s.getOutput()) {
            builder = (builder != null ? builder : s.toBuilder()).setOutput(output);
        }
        StringValue customStatus = maybeResolve(s.getCustomStatus());
        if (customStatus != s.getCustomStatus()) {
            builder = (builder != null ? builder : s.toBuilder()).setCustomStatus(customStatus);
        }
        if (s.hasFailureDetails()) {
            TaskFailureDetails fd = resolveFailureDetails(s.getFailureDetails());
            if (fd != s.getFailureDetails()) {
                builder = (builder != null ? builder : s.toBuilder()).setFailureDetails(fd);
            }
        }

        return builder != null ? builder.build() : s;
    }

    /**
     * Inbound counterpart of {@link #externalizeFailureDetails(TaskFailureDetails)}:
     * resolves any externalized {@code stackTrace} tokens and recurses into
     * {@code innerFailure}.
     */
    private TaskFailureDetails resolveFailureDetails(TaskFailureDetails fd) {
        if (fd == null) {
            return fd;
        }
        TaskFailureDetails.Builder builder = null;
        if (fd.hasStackTrace()) {
            StringValue stack = maybeResolve(fd.getStackTrace());
            if (stack != fd.getStackTrace()) {
                builder = fd.toBuilder().setStackTrace(stack);
            }
        }
        if (fd.hasInnerFailure()) {
            TaskFailureDetails inner = resolveFailureDetails(fd.getInnerFailure());
            if (inner != fd.getInnerFailure()) {
                builder = (builder != null ? builder : fd.toBuilder()).setInnerFailure(inner);
            }
        }
        return builder != null ? builder.build() : fd;
    }

    private HistoryEvent resolveEventPayloads(HistoryEvent e) {
        switch (e.getEventTypeCase()) {
            case EXECUTIONSTARTED:
                if (e.hasExecutionStarted()) {
                    ExecutionStartedEvent es = e.getExecutionStarted();
                    StringValue input = maybeResolve(es.getInput());
                    if (input != es.getInput()) {
                        return e.toBuilder().setExecutionStarted(es.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONCOMPLETED:
                if (e.hasExecutionCompleted()) {
                    ExecutionCompletedEvent ec = e.getExecutionCompleted();
                    ExecutionCompletedEvent.Builder ecb = null;
                    StringValue result = maybeResolve(ec.getResult());
                    if (result != ec.getResult()) {
                        ecb = ec.toBuilder().setResult(result);
                    }
                    if (ec.hasFailureDetails()) {
                        TaskFailureDetails fd = resolveFailureDetails(ec.getFailureDetails());
                        if (fd != ec.getFailureDetails()) {
                            ecb = (ecb != null ? ecb : ec.toBuilder()).setFailureDetails(fd);
                        }
                    }
                    if (ecb != null) {
                        return e.toBuilder().setExecutionCompleted(ecb).build();
                    }
                }
                break;
            case EVENTRAISED:
                if (e.hasEventRaised()) {
                    EventRaisedEvent er = e.getEventRaised();
                    StringValue input = maybeResolve(er.getInput());
                    if (input != er.getInput()) {
                        return e.toBuilder().setEventRaised(er.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case TASKSCHEDULED:
                if (e.hasTaskScheduled()) {
                    TaskScheduledEvent ts = e.getTaskScheduled();
                    StringValue input = maybeResolve(ts.getInput());
                    if (input != ts.getInput()) {
                        return e.toBuilder().setTaskScheduled(ts.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case TASKCOMPLETED:
                if (e.hasTaskCompleted()) {
                    TaskCompletedEvent tc = e.getTaskCompleted();
                    StringValue result = maybeResolve(tc.getResult());
                    if (result != tc.getResult()) {
                        return e.toBuilder().setTaskCompleted(tc.toBuilder().setResult(result)).build();
                    }
                }
                break;
            case TASKFAILED:
                if (e.hasTaskFailed() && e.getTaskFailed().hasFailureDetails()) {
                    TaskFailedEvent tf = e.getTaskFailed();
                    TaskFailureDetails fd = resolveFailureDetails(tf.getFailureDetails());
                    if (fd != tf.getFailureDetails()) {
                        return e.toBuilder().setTaskFailed(tf.toBuilder().setFailureDetails(fd)).build();
                    }
                }
                break;
            case SUBORCHESTRATIONINSTANCECREATED:
                if (e.hasSubOrchestrationInstanceCreated()) {
                    SubOrchestrationInstanceCreatedEvent soc = e.getSubOrchestrationInstanceCreated();
                    StringValue input = maybeResolve(soc.getInput());
                    if (input != soc.getInput()) {
                        return e.toBuilder().setSubOrchestrationInstanceCreated(
                            soc.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case SUBORCHESTRATIONINSTANCECOMPLETED:
                if (e.hasSubOrchestrationInstanceCompleted()) {
                    SubOrchestrationInstanceCompletedEvent sox = e.getSubOrchestrationInstanceCompleted();
                    StringValue result = maybeResolve(sox.getResult());
                    if (result != sox.getResult()) {
                        return e.toBuilder().setSubOrchestrationInstanceCompleted(
                            sox.toBuilder().setResult(result)).build();
                    }
                }
                break;
            case SUBORCHESTRATIONINSTANCEFAILED:
                if (e.hasSubOrchestrationInstanceFailed() && e.getSubOrchestrationInstanceFailed().hasFailureDetails()) {
                    SubOrchestrationInstanceFailedEvent sof = e.getSubOrchestrationInstanceFailed();
                    TaskFailureDetails fd = resolveFailureDetails(sof.getFailureDetails());
                    if (fd != sof.getFailureDetails()) {
                        return e.toBuilder().setSubOrchestrationInstanceFailed(
                            sof.toBuilder().setFailureDetails(fd)).build();
                    }
                }
                break;
            case EVENTSENT:
                if (e.hasEventSent()) {
                    EventSentEvent esent = e.getEventSent();
                    StringValue input = maybeResolve(esent.getInput());
                    if (input != esent.getInput()) {
                        return e.toBuilder().setEventSent(esent.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case GENERICEVENT:
                if (e.hasGenericEvent()) {
                    GenericEvent ge = e.getGenericEvent();
                    StringValue data = maybeResolve(ge.getData());
                    if (data != ge.getData()) {
                        return e.toBuilder().setGenericEvent(ge.toBuilder().setData(data)).build();
                    }
                }
                break;
            case CONTINUEASNEW:
                if (e.hasContinueAsNew()) {
                    ContinueAsNewEvent can = e.getContinueAsNew();
                    StringValue input = maybeResolve(can.getInput());
                    if (input != can.getInput()) {
                        return e.toBuilder().setContinueAsNew(can.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONTERMINATED:
                if (e.hasExecutionTerminated()) {
                    ExecutionTerminatedEvent et = e.getExecutionTerminated();
                    StringValue input = maybeResolve(et.getInput());
                    if (input != et.getInput()) {
                        return e.toBuilder().setExecutionTerminated(et.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONSUSPENDED:
                if (e.hasExecutionSuspended()) {
                    ExecutionSuspendedEvent esus = e.getExecutionSuspended();
                    StringValue input = maybeResolve(esus.getInput());
                    if (input != esus.getInput()) {
                        return e.toBuilder().setExecutionSuspended(esus.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONRESUMED:
                if (e.hasExecutionResumed()) {
                    ExecutionResumedEvent eres = e.getExecutionResumed();
                    StringValue input = maybeResolve(eres.getInput());
                    if (input != eres.getInput()) {
                        return e.toBuilder().setExecutionResumed(eres.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case ENTITYOPERATIONSIGNALED:
                if (e.hasEntityOperationSignaled()) {
                    EntityOperationSignaledEvent eos = e.getEntityOperationSignaled();
                    StringValue input = maybeResolve(eos.getInput());
                    if (input != eos.getInput()) {
                        return e.toBuilder().setEntityOperationSignaled(
                            eos.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case ENTITYOPERATIONCALLED:
                if (e.hasEntityOperationCalled()) {
                    EntityOperationCalledEvent eoc = e.getEntityOperationCalled();
                    StringValue input = maybeResolve(eoc.getInput());
                    if (input != eoc.getInput()) {
                        return e.toBuilder().setEntityOperationCalled(
                            eoc.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case ENTITYOPERATIONCOMPLETED:
                if (e.hasEntityOperationCompleted()) {
                    EntityOperationCompletedEvent ecomp = e.getEntityOperationCompleted();
                    StringValue output = maybeResolve(ecomp.getOutput());
                    if (output != ecomp.getOutput()) {
                        return e.toBuilder().setEntityOperationCompleted(
                            ecomp.toBuilder().setOutput(output)).build();
                    }
                }
                break;
            case HISTORYSTATE:
                if (e.hasHistoryState()) {
                    HistoryStateEvent hs = e.getHistoryState();
                    if (hs.hasOrchestrationState()) {
                        OrchestrationState resolved = resolveOrchestrationState(hs.getOrchestrationState());
                        if (resolved != hs.getOrchestrationState()) {
                            return e.toBuilder().setHistoryState(
                                hs.toBuilder().setOrchestrationState(resolved)).build();
                        }
                    }
                }
                break;
            default:
                // No payload fields to resolve for this event type
                break;
        }
        return e;
    }

    /**
     * Outbound counterpart of {@link #resolveEventPayloads(HistoryEvent)}: walks the
     * same payload fields but uploads oversized values via {@link #maybeExternalize}.
     * Used for {@code CompleteOrchestrationAction.carryoverEvents} so continue-as-new
     * does not send oversized payloads to the sidecar.
     */
    private HistoryEvent externalizeEventPayloads(HistoryEvent e) {
        switch (e.getEventTypeCase()) {
            case EXECUTIONSTARTED:
                if (e.hasExecutionStarted()) {
                    ExecutionStartedEvent es = e.getExecutionStarted();
                    StringValue input = maybeExternalize(es.getInput());
                    if (input != es.getInput()) {
                        return e.toBuilder().setExecutionStarted(es.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONCOMPLETED:
                if (e.hasExecutionCompleted()) {
                    ExecutionCompletedEvent ec = e.getExecutionCompleted();
                    ExecutionCompletedEvent.Builder ecb = null;
                    StringValue result = maybeExternalize(ec.getResult());
                    if (result != ec.getResult()) {
                        ecb = ec.toBuilder().setResult(result);
                    }
                    if (ec.hasFailureDetails()) {
                        TaskFailureDetails fd = externalizeFailureDetails(ec.getFailureDetails());
                        if (fd != ec.getFailureDetails()) {
                            ecb = (ecb != null ? ecb : ec.toBuilder()).setFailureDetails(fd);
                        }
                    }
                    if (ecb != null) {
                        return e.toBuilder().setExecutionCompleted(ecb).build();
                    }
                }
                break;
            case EVENTRAISED:
                if (e.hasEventRaised()) {
                    EventRaisedEvent er = e.getEventRaised();
                    StringValue input = maybeExternalize(er.getInput());
                    if (input != er.getInput()) {
                        return e.toBuilder().setEventRaised(er.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case TASKSCHEDULED:
                if (e.hasTaskScheduled()) {
                    TaskScheduledEvent ts = e.getTaskScheduled();
                    StringValue input = maybeExternalize(ts.getInput());
                    if (input != ts.getInput()) {
                        return e.toBuilder().setTaskScheduled(ts.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case TASKCOMPLETED:
                if (e.hasTaskCompleted()) {
                    TaskCompletedEvent tc = e.getTaskCompleted();
                    StringValue result = maybeExternalize(tc.getResult());
                    if (result != tc.getResult()) {
                        return e.toBuilder().setTaskCompleted(tc.toBuilder().setResult(result)).build();
                    }
                }
                break;
            case TASKFAILED:
                if (e.hasTaskFailed() && e.getTaskFailed().hasFailureDetails()) {
                    TaskFailedEvent tf = e.getTaskFailed();
                    TaskFailureDetails fd = externalizeFailureDetails(tf.getFailureDetails());
                    if (fd != tf.getFailureDetails()) {
                        return e.toBuilder().setTaskFailed(tf.toBuilder().setFailureDetails(fd)).build();
                    }
                }
                break;
            case SUBORCHESTRATIONINSTANCECREATED:
                if (e.hasSubOrchestrationInstanceCreated()) {
                    SubOrchestrationInstanceCreatedEvent soc = e.getSubOrchestrationInstanceCreated();
                    StringValue input = maybeExternalize(soc.getInput());
                    if (input != soc.getInput()) {
                        return e.toBuilder().setSubOrchestrationInstanceCreated(
                            soc.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case SUBORCHESTRATIONINSTANCECOMPLETED:
                if (e.hasSubOrchestrationInstanceCompleted()) {
                    SubOrchestrationInstanceCompletedEvent sox = e.getSubOrchestrationInstanceCompleted();
                    StringValue result = maybeExternalize(sox.getResult());
                    if (result != sox.getResult()) {
                        return e.toBuilder().setSubOrchestrationInstanceCompleted(
                            sox.toBuilder().setResult(result)).build();
                    }
                }
                break;
            case SUBORCHESTRATIONINSTANCEFAILED:
                if (e.hasSubOrchestrationInstanceFailed() && e.getSubOrchestrationInstanceFailed().hasFailureDetails()) {
                    SubOrchestrationInstanceFailedEvent sof = e.getSubOrchestrationInstanceFailed();
                    TaskFailureDetails fd = externalizeFailureDetails(sof.getFailureDetails());
                    if (fd != sof.getFailureDetails()) {
                        return e.toBuilder().setSubOrchestrationInstanceFailed(
                            sof.toBuilder().setFailureDetails(fd)).build();
                    }
                }
                break;
            case EVENTSENT:
                if (e.hasEventSent()) {
                    EventSentEvent esent = e.getEventSent();
                    StringValue input = maybeExternalize(esent.getInput());
                    if (input != esent.getInput()) {
                        return e.toBuilder().setEventSent(esent.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case GENERICEVENT:
                if (e.hasGenericEvent()) {
                    GenericEvent ge = e.getGenericEvent();
                    StringValue data = maybeExternalize(ge.getData());
                    if (data != ge.getData()) {
                        return e.toBuilder().setGenericEvent(ge.toBuilder().setData(data)).build();
                    }
                }
                break;
            case CONTINUEASNEW:
                if (e.hasContinueAsNew()) {
                    ContinueAsNewEvent can = e.getContinueAsNew();
                    StringValue input = maybeExternalize(can.getInput());
                    if (input != can.getInput()) {
                        return e.toBuilder().setContinueAsNew(can.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONTERMINATED:
                if (e.hasExecutionTerminated()) {
                    ExecutionTerminatedEvent et = e.getExecutionTerminated();
                    StringValue input = maybeExternalize(et.getInput());
                    if (input != et.getInput()) {
                        return e.toBuilder().setExecutionTerminated(et.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONSUSPENDED:
                if (e.hasExecutionSuspended()) {
                    ExecutionSuspendedEvent esus = e.getExecutionSuspended();
                    StringValue input = maybeExternalize(esus.getInput());
                    if (input != esus.getInput()) {
                        return e.toBuilder().setExecutionSuspended(esus.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case EXECUTIONRESUMED:
                if (e.hasExecutionResumed()) {
                    ExecutionResumedEvent eres = e.getExecutionResumed();
                    StringValue input = maybeExternalize(eres.getInput());
                    if (input != eres.getInput()) {
                        return e.toBuilder().setExecutionResumed(eres.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case ENTITYOPERATIONSIGNALED:
                if (e.hasEntityOperationSignaled()) {
                    EntityOperationSignaledEvent eos = e.getEntityOperationSignaled();
                    StringValue input = maybeExternalize(eos.getInput());
                    if (input != eos.getInput()) {
                        return e.toBuilder().setEntityOperationSignaled(
                            eos.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case ENTITYOPERATIONCALLED:
                if (e.hasEntityOperationCalled()) {
                    EntityOperationCalledEvent eoc = e.getEntityOperationCalled();
                    StringValue input = maybeExternalize(eoc.getInput());
                    if (input != eoc.getInput()) {
                        return e.toBuilder().setEntityOperationCalled(
                            eoc.toBuilder().setInput(input)).build();
                    }
                }
                break;
            case ENTITYOPERATIONCOMPLETED:
                if (e.hasEntityOperationCompleted()) {
                    EntityOperationCompletedEvent ecomp = e.getEntityOperationCompleted();
                    StringValue output = maybeExternalize(ecomp.getOutput());
                    if (output != ecomp.getOutput()) {
                        return e.toBuilder().setEntityOperationCompleted(
                            ecomp.toBuilder().setOutput(output)).build();
                    }
                }
                break;
            default:
                // No payload fields to externalize for this event type
                break;
        }
        return e;
    }

    /**
     * Externalizes large {@code stackTrace} content on a {@link TaskFailureDetails},
     * recursing into {@code innerFailure}. Returns the input reference unchanged when
     * no field exceeds the threshold.
     */
    private TaskFailureDetails externalizeFailureDetails(TaskFailureDetails fd) {
        if (fd == null) {
            return fd;
        }
        TaskFailureDetails.Builder builder = null;
        if (fd.hasStackTrace()) {
            StringValue stack = maybeExternalize(fd.getStackTrace());
            if (stack != fd.getStackTrace()) {
                builder = fd.toBuilder().setStackTrace(stack);
            }
        }
        if (fd.hasInnerFailure()) {
            TaskFailureDetails inner = externalizeFailureDetails(fd.getInnerFailure());
            if (inner != fd.getInnerFailure()) {
                builder = (builder != null ? builder : fd.toBuilder()).setInnerFailure(inner);
            }
        }
        return builder != null ? builder.build() : fd;
    }

    // ==================== Helpers ====================

    /**
     * Externalizes a StringValue payload if it exceeds the threshold.
     * Returns the original StringValue reference if no change is needed,
     * or a new StringValue containing the blob token.
     */
    private StringValue maybeExternalize(StringValue value) {
        if (value == null || !value.isInitialized() || value.getValue().isEmpty()) {
            return value;
        }

        String strValue = value.getValue();
        int size = utf8ByteLength(strValue);

        if (size < this.options.getThresholdBytes()) {
            return value;
        }

        // Enforce hard cap
        if (size > this.options.getMaxPayloadBytes()) {
            throw new PayloadStorageException(
                "Payload size " + (size / 1024) + " KB exceeds the configured maximum of " +
                (this.options.getMaxPayloadBytes() / 1024) + " KB. " +
                "Reduce the payload size or increase the max payload size limit.");
        }

        String token = this.payloadStore.upload(strValue);
        return StringValue.of(token);
    }

    /**
     * Resolves a StringValue that may contain a payload token.
     * Returns the original StringValue reference if no resolution is needed.
     */
    private StringValue maybeResolve(StringValue value) {
        if (value == null || !value.isInitialized() || value.getValue().isEmpty()) {
            return value;
        }
        String strValue = value.getValue();
        if (!this.payloadStore.isKnownPayloadToken(strValue)) {
            return value;
        }
        String resolved = this.payloadStore.download(strValue);
        return StringValue.of(resolved);
    }

    /**
     * Determines whether an exception represents a permanent storage failure.
     */
    static boolean isPermanentStorageFailure(Exception ex) {
        if (ex instanceof PayloadStorageException) {
            return true;
        }
        if (ex instanceof BlobStorageException) {
            int status = ((BlobStorageException) ex).getStatusCode();
            // 4xx errors are permanent except 408 (timeout) and 429 (throttle)
            return status >= 400 && status < 500 && status != 408 && status != 429;
        }
        return false;
    }

    /**
     * Attempts to fail a {@link WorkItem} whose inbound payloads cannot be resolved
     * (e.g. blob 404 from retention/cleanup, malformed token, container mismatch) by
     * sending a non-retriable completion back to the sidecar. This avoids an infinite
     * replay loop where the sidecar keeps re-dispatching the poisoned work item every
     * time the worker reconnects after an onMessage failure.
     * <p>
     * Completions are sent over the underlying channel (bypassing this interceptor)
     * to avoid re-entering the resolve path with synthesized failure payloads.
     *
     * @return {@code true} if the work item was completed as failed and can be dropped
     *         from the stream; {@code false} if no completion path exists for this
     *         work item variant (caller should surface the error instead).
     */
    private boolean tryFailWorkItem(WorkItem wi, Exception cause, Channel underlying) {
        String completionToken = wi.getCompletionToken();
        TaskFailureDetails failureDetails = TaskFailureDetails.newBuilder()
            .setErrorType(cause.getClass().getName())
            .setErrorMessage(
                "Permanent payload storage failure while resolving externalized payload: "
                    + cause.getMessage())
            .setStackTrace(StringValue.of(getStackTraceString(cause)))
            .setIsNonRetriable(true)
            .build();

        try {
            TaskHubSidecarServiceBlockingStub stub = TaskHubSidecarServiceGrpc.newBlockingStub(underlying);
            if (wi.hasActivityRequest()) {
                ActivityRequest ar = wi.getActivityRequest();
                ActivityResponse response = ActivityResponse.newBuilder()
                    .setInstanceId(ar.getOrchestrationInstance().getInstanceId())
                    .setTaskId(ar.getTaskId())
                    .setFailureDetails(failureDetails)
                    .setCompletionToken(completionToken)
                    .build();
                logger.log(Level.SEVERE,
                    "Permanent payload resolve failure on activity work item (instanceId="
                        + response.getInstanceId() + ", taskId=" + ar.getTaskId()
                        + "); completing as non-retriable failed to avoid replay loop.",
                    cause);
                stub.completeActivityTask(response);
                return true;
            }
            if (wi.hasOrchestratorRequest()) {
                OrchestratorRequest or = wi.getOrchestratorRequest();
                OrchestratorResponse response = OrchestratorResponse.newBuilder()
                    .setInstanceId(or.getInstanceId())
                    .setCompletionToken(completionToken)
                    .addActions(OrchestratorAction.newBuilder()
                        .setCompleteOrchestration(CompleteOrchestrationAction.newBuilder()
                            .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED)
                            .setFailureDetails(failureDetails)
                            .build())
                        .build())
                    .build();
                logger.log(Level.SEVERE,
                    "Permanent payload resolve failure on orchestrator work item (instanceId="
                        + or.getInstanceId() + "); completing as non-retriable failed to avoid replay loop.",
                    cause);
                stub.completeOrchestratorTask(response);
                return true;
            }
            if (wi.hasEntityRequest() || wi.hasEntityRequestV2()) {
                EntityBatchResult response = EntityBatchResult.newBuilder()
                    .setFailureDetails(failureDetails)
                    .setCompletionToken(completionToken)
                    .build();
                logger.log(Level.SEVERE,
                    "Permanent payload resolve failure on entity work item; completing as "
                        + "non-retriable failed to avoid replay loop.",
                    cause);
                stub.completeEntityTask(response);
                return true;
            }
            // HealthPing or unknown variant — no completion path; surface error to caller.
            return false;
        } catch (Exception completionEx) {
            // If we can't even send the failure completion, fall through and let the
            // caller surface the resolve error to the stream so the worker reconnects.
            logger.log(Level.SEVERE,
                "Failed to send non-retriable failure completion for poisoned work item; "
                    + "stream will be terminated and sidecar may re-dispatch.",
                completionEx);
            return false;
        }
    }

    private static String getStackTraceString(Throwable t) {
        java.io.StringWriter sw = new java.io.StringWriter();
        t.printStackTrace(new java.io.PrintWriter(sw));
        return sw.toString();
    }

    /**
     * Computes the UTF-8 encoded byte length of a string without allocating a byte array.
     * <p>
     * Iterates the char sequence and counts bytes per the UTF-8 encoding rules,
     * correctly handling surrogate pairs as 4-byte sequences.
     */
    private static int utf8ByteLength(String s) {
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c <= 0x7F) {
                count++;
            } else if (c <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(c)) {
                count += 4;
                i++; // skip the low surrogate
            } else {
                count += 3;
            }
        }
        return count;
    }
}
