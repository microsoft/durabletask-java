// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;

import io.grpc.*;

import java.nio.charset.StandardCharsets;
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
        this.payloadStore = payloadStore;
        this.options = options;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                Listener<RespT> wrappedListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                        responseListener) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public void onMessage(RespT message) {
                        RespT resolved = (RespT) resolveResponsePayloads(message);
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
            StringValue externalized = maybeExternalize(r.getResult());
            if (externalized != r.getResult()) {
                return r.toBuilder().setResult(externalized).build();
            }
        } catch (Exception ex) {
            if (isPermanentStorageFailure(ex)) {
                // Convert to a failure response so the orchestration sees a failed activity
                return r.toBuilder()
                    .clearResult()
                    .setFailureDetails(TaskFailureDetails.newBuilder()
                        .setErrorType(ex.getClass().getName())
                        .setErrorMessage(ex.getMessage())
                        .setStackTrace(StringValue.of(getStackTraceString(ex)))
                        .setIsNonRetriable(true)
                        .build())
                    .build();
            }
            throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
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
            if (isPermanentStorageFailure(ex)) {
                // Replace with a single Failed completion
                return OrchestratorResponse.newBuilder()
                    .setInstanceId(r.getInstanceId())
                    .setCompletionToken(r.getCompletionToken())
                    .setIsPartial(false)
                    .addActions(OrchestratorAction.newBuilder()
                        .setCompleteOrchestration(CompleteOrchestrationAction.newBuilder()
                            .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED)
                            .setFailureDetails(TaskFailureDetails.newBuilder()
                                .setErrorType(ex.getClass().getName())
                                .setErrorMessage(ex.getMessage())
                                .setStackTrace(StringValue.of(getStackTraceString(ex)))
                                .setIsNonRetriable(true)
                                .build())
                            .build())
                        .build())
                    .build();
            }
            throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    private OrchestratorAction externalizeOrchestratorAction(OrchestratorAction action) {
        OrchestratorAction.Builder builder = null;

        if (action.hasCompleteOrchestration()) {
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
            if (cb != null) {
                builder = action.toBuilder().setCompleteOrchestration(cb.build());
            }
        }

        if (action.hasTerminateOrchestration()) {
            TerminateOrchestrationAction term = action.getTerminateOrchestration();
            StringValue reason = maybeExternalize(term.getReason());
            if (reason != term.getReason()) {
                builder = (builder != null ? builder : action.toBuilder())
                    .setTerminateOrchestration(term.toBuilder().setReason(reason).build());
            }
        }

        if (action.hasScheduleTask()) {
            ScheduleTaskAction schedule = action.getScheduleTask();
            StringValue input = maybeExternalize(schedule.getInput());
            if (input != schedule.getInput()) {
                builder = (builder != null ? builder : action.toBuilder())
                    .setScheduleTask(schedule.toBuilder().setInput(input).build());
            }
        }

        if (action.hasCreateSubOrchestration()) {
            CreateSubOrchestrationAction sub = action.getCreateSubOrchestration();
            StringValue input = maybeExternalize(sub.getInput());
            if (input != sub.getInput()) {
                builder = (builder != null ? builder : action.toBuilder())
                    .setCreateSubOrchestration(sub.toBuilder().setInput(input).build());
            }
        }

        if (action.hasSendEvent()) {
            SendEventAction sendEvt = action.getSendEvent();
            StringValue data = maybeExternalize(sendEvt.getData());
            if (data != sendEvt.getData()) {
                builder = (builder != null ? builder : action.toBuilder())
                    .setSendEvent(sendEvt.toBuilder().setData(data).build());
            }
        }

        if (action.hasSendEntityMessage()) {
            SendEntityMessageAction entityMsg = action.getSendEntityMessage();
            SendEntityMessageAction.Builder emBuilder = null;

            if (entityMsg.hasEntityOperationSignaled()) {
                EntityOperationSignaledEvent sig = entityMsg.getEntityOperationSignaled();
                StringValue input = maybeExternalize(sig.getInput());
                if (input != sig.getInput()) {
                    emBuilder = entityMsg.toBuilder()
                        .setEntityOperationSignaled(sig.toBuilder().setInput(input).build());
                }
            }
            if (entityMsg.hasEntityOperationCalled()) {
                EntityOperationCalledEvent called = entityMsg.getEntityOperationCalled();
                StringValue input = maybeExternalize(called.getInput());
                if (input != called.getInput()) {
                    emBuilder = (emBuilder != null ? emBuilder : entityMsg.toBuilder())
                        .setEntityOperationCalled(called.toBuilder().setInput(input).build());
                }
            }
            if (emBuilder != null) {
                builder = (builder != null ? builder : action.toBuilder())
                    .setSendEntityMessage(emBuilder.build());
            }
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

        return builder != null ? builder.build() : s;
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
                    StringValue result = maybeResolve(ec.getResult());
                    if (result != ec.getResult()) {
                        return e.toBuilder().setExecutionCompleted(ec.toBuilder().setResult(result)).build();
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
        int size = strValue.getBytes(StandardCharsets.UTF_8).length;

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

    private static String getStackTraceString(Throwable t) {
        java.io.StringWriter sw = new java.io.StringWriter();
        t.printStackTrace(new java.io.PrintWriter(sw));
        return sw.toString();
    }
}
