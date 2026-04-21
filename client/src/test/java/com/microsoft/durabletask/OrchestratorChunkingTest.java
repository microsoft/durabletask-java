// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the auto-chunking logic in {@link DurableTaskGrpcWorker}
 * including {@code completeOrchestratorTaskWithChunking} and {@code validateActionsSize}.
 */
public class OrchestratorChunkingTest {

    /**
     * In-process gRPC server that captures all OrchestratorResponse messages
     * sent to completeOrchestratorTask.
     */
    private final List<OrchestratorResponse> capturedResponses =
        Collections.synchronizedList(new ArrayList<>());

    private Server inProcessServer;
    private ManagedChannel inProcessChannel;

    @BeforeEach
    void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        inProcessServer = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                @Override
                public void completeOrchestratorTask(
                        OrchestratorResponse request,
                        StreamObserver<CompleteTaskResponse> responseObserver) {
                    capturedResponses.add(request);
                    responseObserver.onNext(CompleteTaskResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            })
            .build()
            .start();

        inProcessChannel = InProcessChannelBuilder.forName(serverName)
            .directExecutor()
            .build();
    }

    @AfterEach
    void tearDown() {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
    }

    // ==================== validateActionsSize tests ====================

    @Test
    void validateActionsSize_actionUnderLimit_returnsNull() throws Exception {
        OrchestratorAction smallAction = OrchestratorAction.newBuilder()
            .setScheduleTask(ScheduleTaskAction.newBuilder()
                .setName("SmallActivity")
                .setInput(StringValue.of("small"))
                .build())
            .build();

        List<OrchestratorAction> actions = Collections.singletonList(smallAction);
        TaskFailureDetails result = invokeValidateActionsSize(actions, 4_089_446);

        assertNull(result, "Small action should pass validation");
    }

    @Test
    void validateActionsSize_actionOverLimit_returnsFailureDetails() throws Exception {
        // Create an action with a large payload that exceeds the chunk limit
        String largePayload = new String(new char[2_000_000]).replace('\0', 'X');
        OrchestratorAction oversizedAction = OrchestratorAction.newBuilder()
            .setScheduleTask(ScheduleTaskAction.newBuilder()
                .setName("LargeActivity")
                .setInput(StringValue.of(largePayload))
                .build())
            .build();

        int smallChunkLimit = 1_048_576; // 1 MB
        List<OrchestratorAction> actions = Collections.singletonList(oversizedAction);
        TaskFailureDetails result = invokeValidateActionsSize(actions, smallChunkLimit);

        assertNotNull(result, "Oversized action should fail validation");
        assertTrue(result.getIsNonRetriable());
        assertTrue(result.getErrorMessage().contains("exceeds"));
        assertTrue(result.getErrorMessage().contains("large-payload externalization"));
    }

    @Test
    void validateActionsSize_emptyActions_returnsNull() throws Exception {
        List<OrchestratorAction> actions = Collections.emptyList();
        TaskFailureDetails result = invokeValidateActionsSize(actions, 4_089_446);
        assertNull(result);
    }

    @Test
    void validateActionsSize_multipleActions_oneOversized_returnsFailure() throws Exception {
        String largePayload = new String(new char[2_000_000]).replace('\0', 'X');
        OrchestratorAction smallAction = OrchestratorAction.newBuilder()
            .setScheduleTask(ScheduleTaskAction.newBuilder()
                .setName("Small").setInput(StringValue.of("ok")).build())
            .build();
        OrchestratorAction oversizedAction = OrchestratorAction.newBuilder()
            .setScheduleTask(ScheduleTaskAction.newBuilder()
                .setName("Big").setInput(StringValue.of(largePayload)).build())
            .build();

        List<OrchestratorAction> actions = new ArrayList<>();
        actions.add(smallAction);
        actions.add(oversizedAction);
        TaskFailureDetails result = invokeValidateActionsSize(actions, 1_048_576);

        assertNotNull(result);
        assertTrue(result.getIsNonRetriable());
    }

    // ==================== completeOrchestratorTaskWithChunking tests ====================

    @Test
    void chunking_responseUnderLimit_singleSend() throws Exception {
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(false, 4_089_446);

        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("SmallActivity")
                    .setInput(StringValue.of("small-data"))
                    .build())
                .build())
            .build();

        invokeChunkingMethod(worker, response);

        assertEquals(1, capturedResponses.size(), "Should send single response");
        assertFalse(capturedResponses.get(0).getIsPartial());
        assertFalse(capturedResponses.get(0).hasChunkIndex());
    }

    @Test
    void chunking_responseOverLimit_multipleChunks() throws Exception {
        // Use a very small chunk limit so our actions exceed it
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, smallChunkLimit);

        // Create multiple actions that together exceed the chunk limit
        OrchestratorResponse.Builder responseBuilder = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token");

        // Each action ~200KB, create 10 = ~2MB total > 1MB limit
        for (int i = 0; i < 10; i++) {
            String payload = new String(new char[200_000]).replace('\0', (char) ('A' + i));
            responseBuilder.addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("Activity" + i)
                    .setInput(StringValue.of(payload))
                    .build())
                .build());
        }

        OrchestratorResponse response = responseBuilder.build();
        invokeChunkingMethod(worker, response);

        assertTrue(capturedResponses.size() > 1, "Should produce multiple chunks, got: " + capturedResponses.size());

        // All chunks except the last should have isPartial=true
        for (int i = 0; i < capturedResponses.size() - 1; i++) {
            assertTrue(capturedResponses.get(i).getIsPartial(),
                "Chunk " + i + " should be partial");
        }
        assertFalse(capturedResponses.get(capturedResponses.size() - 1).getIsPartial(),
            "Last chunk should not be partial");

        // All chunks should have chunkIndex set (isChunkedMode activates)
        for (int i = 0; i < capturedResponses.size(); i++) {
            assertTrue(capturedResponses.get(i).hasChunkIndex(),
                "Chunk " + i + " should have chunkIndex");
            assertEquals(i, capturedResponses.get(i).getChunkIndex().getValue());
        }

        // All actions should be accounted for
        int totalActions = 0;
        for (OrchestratorResponse chunk : capturedResponses) {
            totalActions += chunk.getActionsCount();
        }
        assertEquals(10, totalActions, "All 10 actions should be distributed across chunks");

        // All chunks should preserve instanceId and completionToken
        for (OrchestratorResponse chunk : capturedResponses) {
            assertEquals("test-instance", chunk.getInstanceId());
            assertEquals("token", chunk.getCompletionToken());
        }
    }

    @Test
    void chunking_singleOversizedAction_acceptedInFirstChunk_noChunkIndex() throws Exception {
        // A single action that exceeds the chunk limit should be accepted as the first
        // item in the chunk. When only one chunk is needed, isChunkedMode should be false
        // and no chunkIndex should be set.
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, smallChunkLimit);

        String largePayload = new String(new char[2_000_000]).replace('\0', 'Z');
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("OversizedActivity")
                    .setInput(StringValue.of(largePayload))
                    .build())
                .build())
            .build();

        invokeChunkingMethod(worker, response);

        assertEquals(1, capturedResponses.size(), "Single oversized action should produce one chunk");
        assertFalse(capturedResponses.get(0).getIsPartial(), "Should not be partial");
        // isChunkedMode should be false → no chunkIndex
        assertFalse(capturedResponses.get(0).hasChunkIndex(),
            "Single oversized action in one chunk should NOT have chunkIndex set (avoids confusing backend)");
    }

    @Test
    void chunking_oversizedFirstAction_alwaysAccepted() throws Exception {
        // The TryAddAction fix: even if the first action exceeds the chunk limit,
        // it should still be accepted (to avoid infinite loops)
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, smallChunkLimit);

        String largePayload = new String(new char[2_000_000]).replace('\0', 'X');
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("OversizedFirst")
                    .setInput(StringValue.of(largePayload))
                    .build())
                .build())
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("Small")
                    .setInput(StringValue.of("tiny"))
                    .build())
                .build())
            .build();

        invokeChunkingMethod(worker, response);

        assertTrue(capturedResponses.size() >= 2,
            "Should produce at least 2 chunks (oversized first + small second)");

        // First chunk should contain the oversized action
        assertEquals(1, capturedResponses.get(0).getActionsCount(),
            "First chunk should contain exactly the oversized action");
        assertEquals("OversizedFirst",
            capturedResponses.get(0).getActions(0).getScheduleTask().getName());

        // Both actions accounted for
        int totalActions = 0;
        for (OrchestratorResponse chunk : capturedResponses) {
            totalActions += chunk.getActionsCount();
        }
        assertEquals(2, totalActions);
    }

    @Test
    void chunking_firstChunkPreservesTraceContext_subsequentChunksSendZeroEvents() throws Exception {
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, smallChunkLimit);

        OrchestrationTraceContext traceCtx = OrchestrationTraceContext.newBuilder()
            .setSpanID(StringValue.of("1234567890abcdef"))
            .build();

        OrchestratorResponse.Builder responseBuilder = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .setOrchestrationTraceContext(traceCtx);

        // Create enough actions to force multiple chunks
        for (int i = 0; i < 10; i++) {
            String payload = new String(new char[200_000]).replace('\0', (char) ('A' + i));
            responseBuilder.addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("Activity" + i)
                    .setInput(StringValue.of(payload))
                    .build())
                .build());
        }

        invokeChunkingMethod(worker, responseBuilder.build());

        assertTrue(capturedResponses.size() > 1, "Should have multiple chunks");

        // First chunk should have trace context (and no numEventsProcessed)
        assertTrue(capturedResponses.get(0).hasOrchestrationTraceContext(),
            "First chunk should preserve trace context");
        assertFalse(capturedResponses.get(0).hasNumEventsProcessed(),
            "First chunk should not have numEventsProcessed");

        // Subsequent chunks should have numEventsProcessed = 0
        for (int i = 1; i < capturedResponses.size(); i++) {
            assertTrue(capturedResponses.get(i).hasNumEventsProcessed(),
                "Chunk " + i + " should have numEventsProcessed");
            assertEquals(0, capturedResponses.get(i).getNumEventsProcessed().getValue(),
                "Subsequent chunks should have numEventsProcessed = 0");
        }
    }

    @Test
    void chunking_lpDisabled_oversizedAction_sendsFailureResponse() throws Exception {
        // When LP is disabled, validateActionsSize should reject oversized actions
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(false, smallChunkLimit);

        String largePayload = new String(new char[2_000_000]).replace('\0', 'X');
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("OversizedActivity")
                    .setInput(StringValue.of(largePayload))
                    .build())
                .build())
            .build();

        invokeChunkingMethod(worker, response);

        assertEquals(1, capturedResponses.size());
        OrchestratorResponse sent = capturedResponses.get(0);
        assertEquals(1, sent.getActionsCount());
        assertTrue(sent.getActions(0).hasCompleteOrchestration());
        assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED,
            sent.getActions(0).getCompleteOrchestration().getOrchestrationStatus());
        assertTrue(sent.getActions(0).getCompleteOrchestration().getFailureDetails().getIsNonRetriable());
    }

    @Test
    void chunking_lpEnabled_oversizedAction_skipsValidation() throws Exception {
        // When LP is enabled, validateActionsSize is skipped, so oversized actions just go through
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, smallChunkLimit);

        String largePayload = new String(new char[2_000_000]).replace('\0', 'Y');
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("OversizedActivity")
                    .setInput(StringValue.of(largePayload))
                    .build())
                .build())
            .build();

        invokeChunkingMethod(worker, response);

        // Should NOT be a failure response — should just be sent normally
        assertEquals(1, capturedResponses.size());
        OrchestratorResponse sent = capturedResponses.get(0);
        assertEquals(1, sent.getActionsCount());
        assertTrue(sent.getActions(0).hasScheduleTask(), "Should contain the original ScheduleTask, not a failure");
        assertEquals("OversizedActivity", sent.getActions(0).getScheduleTask().getName());
    }

    @Test
    void chunking_preservesCustomStatusAndRequiresHistory() throws Exception {
        int smallChunkLimit = 1_048_576; // 1 MB
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, smallChunkLimit);

        OrchestratorResponse.Builder responseBuilder = OrchestratorResponse.newBuilder()
            .setInstanceId("test-instance")
            .setCompletionToken("token")
            .setCustomStatus(StringValue.of("my-status"))
            .setRequiresHistory(true);

        for (int i = 0; i < 10; i++) {
            String payload = new String(new char[200_000]).replace('\0', (char) ('A' + i));
            responseBuilder.addActions(OrchestratorAction.newBuilder()
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("Activity" + i)
                    .setInput(StringValue.of(payload))
                    .build())
                .build());
        }

        invokeChunkingMethod(worker, responseBuilder.build());

        assertTrue(capturedResponses.size() > 1);
        for (OrchestratorResponse chunk : capturedResponses) {
            assertEquals("my-status", chunk.getCustomStatus().getValue());
            assertTrue(chunk.getRequiresHistory());
        }
    }

    // ==================== Builder tests ====================

    @Test
    void workerBuilder_setMaxChunkSizeBytes_valid() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.setMaxChunkSizeBytes(1_048_576); // 1 MB - minimum
        assertEquals(1_048_576, builder.maxChunkSizeBytes);

        builder.setMaxChunkSizeBytes(4_089_446); // 3.9 MB - maximum
        assertEquals(4_089_446, builder.maxChunkSizeBytes);
    }

    @Test
    void workerBuilder_setMaxChunkSizeBytes_tooSmall_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setMaxChunkSizeBytes(1_000_000));
    }

    @Test
    void workerBuilder_setMaxChunkSizeBytes_tooLarge_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setMaxChunkSizeBytes(5_000_000));
    }

    @Test
    void workerBuilder_defaultMaxChunkSizeBytes() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertEquals(4_089_446, builder.maxChunkSizeBytes);
    }

    @Test
    void workerBuilder_setSupportsLargePayloads() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertFalse(builder.supportsLargePayloads);
        builder.setSupportsLargePayloads(true);
        assertTrue(builder.supportsLargePayloads);
    }

    @Test
    void workerBuilder_addInterceptor_nullThrows() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.addInterceptor(null));
    }

    // ==================== Capability announcement tests ====================

    @Test
    void buildGetWorkItemsRequest_lpEnabled_includesCapability() throws Exception {
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(true, 4_089_446);
        GetWorkItemsRequest request = invokeGetWorkItemsRequest(worker);
        assertTrue(request.getCapabilitiesList().contains(
            WorkerCapability.WORKER_CAPABILITY_LARGE_PAYLOADS),
            "Should include WORKER_CAPABILITY_LARGE_PAYLOADS when LP is enabled");
    }

    @Test
    void buildGetWorkItemsRequest_lpDisabled_noCapability() throws Exception {
        DurableTaskGrpcWorker worker = buildWorkerWithChannel(false, 4_089_446);
        GetWorkItemsRequest request = invokeGetWorkItemsRequest(worker);
        assertFalse(request.getCapabilitiesList().contains(
            WorkerCapability.WORKER_CAPABILITY_LARGE_PAYLOADS),
            "Should NOT include WORKER_CAPABILITY_LARGE_PAYLOADS when LP is disabled");
    }

    // ==================== Helpers ====================

    private DurableTaskGrpcWorker buildWorkerWithChannel(boolean supportsLargePayloads, int maxChunkSizeBytes) {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder()
            .grpcChannel(inProcessChannel);
        builder.supportsLargePayloads = supportsLargePayloads;
        builder.maxChunkSizeBytes = maxChunkSizeBytes;
        return builder.build();
    }

    /**
     * Invokes the private static validateActionsSize method via reflection.
     */
    @SuppressWarnings("unchecked")
    private TaskFailureDetails invokeValidateActionsSize(
            List<OrchestratorAction> actions, int maxChunkBytes) throws Exception {
        Method method = DurableTaskGrpcWorker.class.getDeclaredMethod(
            "validateActionsSize", List.class, int.class, boolean.class, int.class);
        method.setAccessible(true);
        return (TaskFailureDetails) method.invoke(null, actions, maxChunkBytes, false, 0);
    }

    /**
     * Invokes the private completeOrchestratorTaskWithChunking method via reflection.
     */
    private void invokeChunkingMethod(DurableTaskGrpcWorker worker, OrchestratorResponse response) throws Exception {
        Method method = DurableTaskGrpcWorker.class.getDeclaredMethod(
            "completeOrchestratorTaskWithChunking", OrchestratorResponse.class);
        method.setAccessible(true);
        method.invoke(worker, response);
    }

    /**
     * Reads the getWorkItemsRequest field from the worker via reflection.
     */
    private GetWorkItemsRequest invokeGetWorkItemsRequest(DurableTaskGrpcWorker worker) throws Exception {
        Field field = DurableTaskGrpcWorker.class.getDeclaredField("getWorkItemsRequest");
        field.setAccessible(true);
        return (GetWorkItemsRequest) field.get(worker);
    }
}
