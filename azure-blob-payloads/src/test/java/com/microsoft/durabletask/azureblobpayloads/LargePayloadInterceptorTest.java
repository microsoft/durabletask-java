// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link LargePayloadInterceptor} — verifies externalization and resolution logic
 * for all proto message types listed in the design plan section 2.6.
 */
class LargePayloadInterceptorTest {

    private PayloadStore mockStore;
    private LargePayloadStorageOptions options;
    private LargePayloadInterceptor interceptor;

    @BeforeEach
    void setUp() {
        mockStore = mock(PayloadStore.class);
        options = new LargePayloadStorageOptions()
            .setThresholdBytes(10) // low threshold so test payloads trigger externalization
            .setMaxPayloadBytes(10 * 1024 * 1024);
        interceptor = new LargePayloadInterceptor(mockStore, options);

        // By default, upload returns a token
        when(mockStore.upload(anyString())).thenAnswer(inv ->
            "blob:v1:durabletask-payloads:" + inv.getArgument(0).hashCode());

        // By default, download resolves the token back
        when(mockStore.download(anyString())).thenReturn("resolved-payload");
        when(mockStore.isKnownPayloadToken(anyString())).thenAnswer(inv -> {
            String val = inv.getArgument(0);
            return val != null && val.startsWith("blob:v1:");
        });
    }

    // ==================== Externalization tests ====================

    @Test
    void externalize_CreateInstanceRequest_input() {
        CreateInstanceRequest request = CreateInstanceRequest.newBuilder()
            .setInstanceId("test")
            .setInput(StringValue.of("large-payload-data"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof CreateInstanceRequest);
        CreateInstanceRequest r = (CreateInstanceRequest) result;
        assertTrue(r.getInput().getValue().startsWith("blob:v1:"));
        verify(mockStore).upload("large-payload-data");
    }

    @Test
    void externalize_RaiseEventRequest_input() {
        RaiseEventRequest request = RaiseEventRequest.newBuilder()
            .setInstanceId("test")
            .setInput(StringValue.of("event-data"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof RaiseEventRequest);
        assertTrue(((RaiseEventRequest) result).getInput().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_TerminateRequest_output() {
        TerminateRequest request = TerminateRequest.newBuilder()
            .setInstanceId("test")
            .setOutput(StringValue.of("terminate-reason"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof TerminateRequest);
        assertTrue(((TerminateRequest) result).getOutput().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_SuspendRequest_reason() {
        SuspendRequest request = SuspendRequest.newBuilder()
            .setInstanceId("test")
            .setReason(StringValue.of("suspend-reason"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof SuspendRequest);
        assertTrue(((SuspendRequest) result).getReason().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_ResumeRequest_reason() {
        ResumeRequest request = ResumeRequest.newBuilder()
            .setInstanceId("test")
            .setReason(StringValue.of("resume-reason"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof ResumeRequest);
        assertTrue(((ResumeRequest) result).getReason().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_smallPayload_notExternalized() {
        // Payload under threshold → should not be uploaded
        options.setThresholdBytes(1_000_000);
        interceptor = new LargePayloadInterceptor(mockStore, options);

        CreateInstanceRequest request = CreateInstanceRequest.newBuilder()
            .setInstanceId("test")
            .setInput(StringValue.of("small"))
            .build();

        Object result = invokeExternalize(request);
        assertSame(request, result);
        verify(mockStore, never()).upload(anyString());
    }

    @Test
    void externalize_emptyInput_notExternalized() {
        CreateInstanceRequest request = CreateInstanceRequest.newBuilder()
            .setInstanceId("test")
            .setInput(StringValue.of(""))
            .build();

        Object result = invokeExternalize(request);
        assertSame(request, result);
        verify(mockStore, never()).upload(anyString());
    }

    @Test
    void externalize_exceedsMaxPayload_throws() {
        // Configure a consistent pair: threshold <= max (interceptor invariant),
        // and a payload that exceeds max so the size check fires.
        options.setThresholdBytes(5).setMaxPayloadBytes(5);
        interceptor = new LargePayloadInterceptor(mockStore, options);

        CreateInstanceRequest request = CreateInstanceRequest.newBuilder()
            .setInstanceId("test")
            .setInput(StringValue.of("this-exceeds-the-max"))
            .build();

        assertThrows(PayloadStorageException.class, () -> invokeExternalize(request));
    }

    @Test
    void externalize_ActivityResponse_permanentFailure_convertedToFailureDetails() {
        when(mockStore.upload(anyString())).thenThrow(
            new PayloadStorageException("Payload too large"));

        ActivityResponse request = ActivityResponse.newBuilder()
            .setInstanceId("test")
            .setTaskId(1)
            .setResult(StringValue.of("large-activity-result"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof ActivityResponse);
        ActivityResponse r = (ActivityResponse) result;
        assertTrue(r.hasFailureDetails());
        assertTrue(r.getFailureDetails().getIsNonRetriable());
        assertFalse(r.hasResult());
    }

    @Test
    void externalize_OrchestratorResponse_permanentFailure_becomesFailedCompletion() {
        when(mockStore.upload(anyString())).thenThrow(
            new PayloadStorageException("Storage failure"));

        OrchestratorResponse request = OrchestratorResponse.newBuilder()
            .setInstanceId("test")
            .setCompletionToken("token")
            .setCustomStatus(StringValue.of("large-custom-status"))
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof OrchestratorResponse);
        OrchestratorResponse r = (OrchestratorResponse) result;
        assertEquals(1, r.getActionsCount());
        assertTrue(r.getActions(0).hasCompleteOrchestration());
        assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED,
            r.getActions(0).getCompleteOrchestration().getOrchestrationStatus());
        assertTrue(r.getActions(0).getCompleteOrchestration().getFailureDetails().getIsNonRetriable());
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 429, 503})
    void transientWorkerUploadFailures_propagateWithoutSendingACompletion(int status) {
        BlobStorageException storageFailure = storageFailure(status);
        for (RuntimeException failure : new RuntimeException[] {
                storageFailure, new PayloadStorageException("Storage upload failed", storageFailure)}) {
            assertWorkerCompletionsNotSent(failure);
        }
    }

    @Test
    void unexpectedWorkerUploadFailures_propagateWithoutSendingACompletion() {
        assertWorkerCompletionsNotSent(new UncheckedIOException(new IOException("Connection reset")));
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 404})
    void permanentUploadFailures_completeActivitiesAndOrchestrationsButNotEntities(int status) {
        BlobStorageException storageFailure = storageFailure(status);
        for (RuntimeException failure : new RuntimeException[] {
                storageFailure, new PayloadStorageException("Storage upload failed", storageFailure)}) {
            doThrow(failure).when(mockStore).upload(anyString());
            ActivityResponse activity = (ActivityResponse) invokeExternalize(ActivityResponse.newBuilder()
                    .setResult(StringValue.of("large-activity-result")).build());
            assertTrue(activity.getFailureDetails().getIsNonRetriable());
            assertFalse(activity.hasResult());
            OrchestratorResponse orchestration = (OrchestratorResponse) invokeExternalize(
                    OrchestratorResponse.newBuilder()
                            .setCustomStatus(StringValue.of("large-custom-status")).build());
            assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED,
                    orchestration.getActions(0).getCompleteOrchestration().getOrchestrationStatus());
            assertTrue(orchestration.getActions(0).getCompleteOrchestration()
                    .getFailureDetails().getIsNonRetriable());
            assertRequestNotSent(EntityBatchResult.newBuilder()
                    .setEntityState(StringValue.of("large-entity-state")).build(), failure);
        }
    }

    @Test
    void permanentEntityPayloadSizeFailure_propagatesWithoutSendingACompletion() {
        assertRequestNotSentWithFailure(EntityBatchResult.newBuilder()
                .setEntityState(StringValue.of("large-entity-state")).build(),
                new PayloadStorageException("Payload too large"));
    }

    @Test
    void externalize_OrchestratorAction_ScheduleTask_input() {
        OrchestratorResponse request = OrchestratorResponse.newBuilder()
            .setInstanceId("test")
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("MyActivity")
                    .setInput(StringValue.of("large-activity-input"))
                    .build())
                .build())
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof OrchestratorResponse);
        OrchestratorResponse r = (OrchestratorResponse) result;
        assertTrue(r.getActions(0).getScheduleTask().getInput().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_OrchestratorAction_CreateSubOrchestration_input() {
        OrchestratorResponse request = OrchestratorResponse.newBuilder()
            .setInstanceId("test")
            .addActions(OrchestratorAction.newBuilder()
                .setCreateSubOrchestration(CreateSubOrchestrationAction.newBuilder()
                    .setName("SubOrch")
                    .setInput(StringValue.of("sub-orch-input"))
                    .build())
                .build())
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof OrchestratorResponse);
        OrchestratorResponse r = (OrchestratorResponse) result;
        assertTrue(r.getActions(0).getCreateSubOrchestration().getInput().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_OrchestratorAction_CompleteOrchestration_result() {
        OrchestratorResponse request = OrchestratorResponse.newBuilder()
            .setInstanceId("test")
            .addActions(OrchestratorAction.newBuilder()
                .setCompleteOrchestration(CompleteOrchestrationAction.newBuilder()
                    .setResult(StringValue.of("large-orchestration-result"))
                    .build())
                .build())
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof OrchestratorResponse);
        OrchestratorResponse r = (OrchestratorResponse) result;
        assertTrue(r.getActions(0).getCompleteOrchestration().getResult().getValue().startsWith("blob:v1:"));
    }

    @Test
    void externalize_OrchestratorAction_SendEvent_data() {
        OrchestratorResponse request = OrchestratorResponse.newBuilder()
            .setInstanceId("test")
            .addActions(OrchestratorAction.newBuilder()
                .setSendEvent(SendEventAction.newBuilder()
                    .setData(StringValue.of("large-event-data"))
                    .build())
                .build())
            .build();

        Object result = invokeExternalize(request);
        assertTrue(result instanceof OrchestratorResponse);
        OrchestratorResponse r = (OrchestratorResponse) result;
        assertTrue(r.getActions(0).getSendEvent().getData().getValue().startsWith("blob:v1:"));
    }

    // ==================== Resolution tests ====================

    @Test
    void resolve_GetInstanceResponse_orchestrationState() {
        GetInstanceResponse response = GetInstanceResponse.newBuilder()
            .setExists(true)
            .setOrchestrationState(OrchestrationState.newBuilder()
                .setInstanceId("test")
                .setInput(StringValue.of("blob:v1:container:name1"))
                .setOutput(StringValue.of("blob:v1:container:name2"))
                .setCustomStatus(StringValue.of("blob:v1:container:name3"))
                .build())
            .build();

        Object result = invokeResolve(response);
        assertTrue(result instanceof GetInstanceResponse);
        OrchestrationState state = ((GetInstanceResponse) result).getOrchestrationState();
        assertEquals("resolved-payload", state.getInput().getValue());
        assertEquals("resolved-payload", state.getOutput().getValue());
        assertEquals("resolved-payload", state.getCustomStatus().getValue());
        verify(mockStore, times(3)).download(anyString());
    }

    @Test
    void resolve_QueryInstancesResponse_multipleStates() {
        QueryInstancesResponse response = QueryInstancesResponse.newBuilder()
            .addOrchestrationState(OrchestrationState.newBuilder()
                .setInstanceId("test1")
                .setInput(StringValue.of("blob:v1:container:a"))
                .build())
            .addOrchestrationState(OrchestrationState.newBuilder()
                .setInstanceId("test2")
                .setOutput(StringValue.of("blob:v1:container:b"))
                .build())
            .build();

        Object result = invokeResolve(response);
        assertTrue(result instanceof QueryInstancesResponse);
        QueryInstancesResponse r = (QueryInstancesResponse) result;
        assertEquals("resolved-payload", r.getOrchestrationState(0).getInput().getValue());
        assertEquals("resolved-payload", r.getOrchestrationState(1).getOutput().getValue());
    }

    @Test
    void resolve_nonTokenValue_notResolved() {
        GetInstanceResponse response = GetInstanceResponse.newBuilder()
            .setExists(true)
            .setOrchestrationState(OrchestrationState.newBuilder()
                .setInstanceId("test")
                .setInput(StringValue.of("plain-value"))
                .build())
            .build();

        Object result = invokeResolve(response);
        assertSame(response, result);
        verify(mockStore, never()).download(anyString());
    }

    @Test
    void resolve_WorkItem_activityRequest_input() {
        WorkItem wi = WorkItem.newBuilder()
            .setActivityRequest(ActivityRequest.newBuilder()
                .setName("MyActivity")
                .setInput(StringValue.of("blob:v1:container:activity-input"))
                .build())
            .build();

        Object result = invokeResolve(wi);
        assertTrue(result instanceof WorkItem);
        assertEquals("resolved-payload",
            ((WorkItem) result).getActivityRequest().getInput().getValue());
    }

    @Test
    void resolve_WorkItem_orchestratorRequest_historyEvents() {
        WorkItem wi = WorkItem.newBuilder()
            .setOrchestratorRequest(OrchestratorRequest.newBuilder()
                .setInstanceId("test")
                .addPastEvents(HistoryEvent.newBuilder()
                    .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setInput(StringValue.of("blob:v1:container:es-input"))
                        .build())
                    .build())
                .addNewEvents(HistoryEvent.newBuilder()
                    .setTaskCompleted(TaskCompletedEvent.newBuilder()
                        .setResult(StringValue.of("blob:v1:container:tc-result"))
                        .build())
                    .build())
                .build())
            .build();

        Object result = invokeResolve(wi);
        assertTrue(result instanceof WorkItem);
        WorkItem resolved = (WorkItem) result;
        assertEquals("resolved-payload",
            resolved.getOrchestratorRequest().getPastEvents(0)
                .getExecutionStarted().getInput().getValue());
        assertEquals("resolved-payload",
            resolved.getOrchestratorRequest().getNewEvents(0)
                .getTaskCompleted().getResult().getValue());
    }

    @Test
    void resolve_historyEvent_allTypes() {
        // Verify each history event type with payload fields is resolved
        verifyEventResolution(HistoryEvent.newBuilder()
            .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setExecutionCompleted(ExecutionCompletedEvent.newBuilder()
                .setResult(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setEventRaised(EventRaisedEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setTaskScheduled(TaskScheduledEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setTaskCompleted(TaskCompletedEvent.newBuilder()
                .setResult(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setSubOrchestrationInstanceCreated(SubOrchestrationInstanceCreatedEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setSubOrchestrationInstanceCompleted(SubOrchestrationInstanceCompletedEvent.newBuilder()
                .setResult(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setEventSent(EventSentEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setGenericEvent(GenericEvent.newBuilder()
                .setData(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setContinueAsNew(ContinueAsNewEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setExecutionTerminated(ExecutionTerminatedEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setExecutionSuspended(ExecutionSuspendedEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setExecutionResumed(ExecutionResumedEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setEntityOperationSignaled(EntityOperationSignaledEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setEntityOperationCalled(EntityOperationCalledEvent.newBuilder()
                .setInput(StringValue.of("blob:v1:c:n")).build()).build());
        verifyEventResolution(HistoryEvent.newBuilder()
            .setEntityOperationCompleted(EntityOperationCompletedEvent.newBuilder()
                .setOutput(StringValue.of("blob:v1:c:n")).build()).build());
    }

    @Test
    void isPermanentStorageFailure_payloadStorageException_noCause_returnsTrue() {
        assertTrue(LargePayloadInterceptor.isPermanentStorageFailure(
            new PayloadStorageException("test")));
    }

    @Test
    void isPermanentStorageFailure_payloadStorageException_wrapping404_returnsTrue() {
        BlobStorageException blobEx = mock(BlobStorageException.class);
        when(blobEx.getStatusCode()).thenReturn(404);
        assertTrue(LargePayloadInterceptor.isPermanentStorageFailure(
            new PayloadStorageException("not found", blobEx)));
    }

    @Test
    void isPermanentStorageFailure_payloadStorageException_wrapping503_returnsFalse() {
        BlobStorageException blobEx = mock(BlobStorageException.class);
        when(blobEx.getStatusCode()).thenReturn(503);
        assertFalse(LargePayloadInterceptor.isPermanentStorageFailure(
            new PayloadStorageException("transient", blobEx)));
    }

    @Test
    void isPermanentStorageFailure_payloadStorageException_wrapping429_returnsFalse() {
        BlobStorageException blobEx = mock(BlobStorageException.class);
        when(blobEx.getStatusCode()).thenReturn(429);
        assertFalse(LargePayloadInterceptor.isPermanentStorageFailure(
            new PayloadStorageException("throttled", blobEx)));
    }

    @Test
    void isPermanentStorageFailure_runtimeException_returnsFalse() {
        assertFalse(LargePayloadInterceptor.isPermanentStorageFailure(
            new RuntimeException("test")));
    }

    // ==================== Helpers ====================

    private static BlobStorageException storageFailure(int status) {
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(status);
        return new BlobStorageException("Storage upload failed", response, null);
    }

    private void assertWorkerCompletionsNotSent(RuntimeException failure) {
        TaskFailureDetails details = TaskFailureDetails.newBuilder()
                .setErrorType("OriginalError")
                .setStackTrace(StringValue.of("large-failure-stack-trace"))
                .build();
        Message[] requests = {
                ActivityResponse.newBuilder().setResult(StringValue.of("large-activity-result")).build(),
                ActivityResponse.newBuilder().setFailureDetails(details).build(),
                OrchestratorResponse.newBuilder().setCustomStatus(StringValue.of("large-custom-status")).build(),
                OrchestratorResponse.newBuilder().addActions(OrchestratorAction.newBuilder()
                        .setScheduleTask(ScheduleTaskAction.newBuilder()
                                .setInput(StringValue.of("large-activity-input")))).build(),
                OrchestratorResponse.newBuilder().addActions(OrchestratorAction.newBuilder()
                        .setCompleteOrchestration(CompleteOrchestrationAction.newBuilder()
                                .setFailureDetails(details))).build(),
                EntityBatchResult.newBuilder().setEntityState(StringValue.of("large-entity-state")).build(),
                EntityBatchResult.newBuilder().addResults(OperationResult.newBuilder()
                        .setSuccess(OperationResultSuccess.newBuilder()
                                .setResult(StringValue.of("large-entity-result")))).build(),
                EntityBatchResult.newBuilder().addActions(OperationAction.newBuilder()
                        .setSendSignal(SendSignalAction.newBuilder()
                                .setInput(StringValue.of("large-entity-signal")))).build(),
                EntityBatchResult.newBuilder().setFailureDetails(details).build(),
                EntityBatchResult.newBuilder().addResults(OperationResult.newBuilder()
                        .setFailure(OperationResultFailure.newBuilder().setFailureDetails(details))).build()
        };
        for (Message request : requests) {
            assertRequestNotSentWithFailure(request, failure);
        }
    }

    private <ReqT extends Message> void assertRequestNotSentWithFailure(ReqT request, RuntimeException failure) {
        doThrow(failure).when(mockStore).upload(anyString());
        assertRequestNotSent(request, failure);
    }

    private <ReqT extends Message> void assertRequestNotSent(ReqT request, RuntimeException failure) {
        MethodDescriptor<ReqT, Empty> method = MethodDescriptor.<ReqT, Empty>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("test/CompleteTask")
                .setRequestMarshaller(ProtoUtils.marshaller(request))
                .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
                .build();
        ClientCall<ReqT, Empty> delegate = mock();
        Channel channel = mock(Channel.class);
        when(channel.newCall(method, CallOptions.DEFAULT)).thenReturn(delegate);
        ClientCall<ReqT, Empty> call = interceptor.interceptCall(method, CallOptions.DEFAULT, channel);

        assertSame(failure, assertThrows(RuntimeException.class, () -> call.sendMessage(request)));
        verify(delegate, never()).sendMessage(any());
    }

    /**
     * Uses reflection to invoke the private externalizeRequestPayloads method.
     */
    private Object invokeExternalize(Object request) {
        try {
            java.lang.reflect.Method method = LargePayloadInterceptor.class
                .getDeclaredMethod("externalizeRequestPayloads", Object.class);
            method.setAccessible(true);
            return method.invoke(interceptor, request);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Uses reflection to invoke the private resolveResponsePayloads method.
     */
    private Object invokeResolve(Object response) {
        try {
            java.lang.reflect.Method method = LargePayloadInterceptor.class
                .getDeclaredMethod("resolveResponsePayloads", Object.class);
            method.setAccessible(true);
            return method.invoke(interceptor, response);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wraps a history event in a WorkItem orchestrator request, resolves it, and verifies
     * the download was called.
     */
    private void verifyEventResolution(HistoryEvent event) {
        reset(mockStore);
        when(mockStore.download(anyString())).thenReturn("resolved");
        when(mockStore.isKnownPayloadToken(anyString())).thenAnswer(inv -> {
            String val = inv.getArgument(0);
            return val != null && val.startsWith("blob:v1:");
        });

        WorkItem wi = WorkItem.newBuilder()
            .setOrchestratorRequest(OrchestratorRequest.newBuilder()
                .setInstanceId("test")
                .addNewEvents(event)
                .build())
            .build();

        Object result = invokeResolve(wi);
        assertNotSame(wi, result, "Event type " + event.getEventTypeCase() + " was not resolved");
        verify(mockStore, atLeastOnce()).download(anyString());
    }
}
