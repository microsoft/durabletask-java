// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.CreateInstanceRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.CreateInstanceResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.CreateTaskHubRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.CreateTaskHubResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.DeleteTaskHubRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.DeleteTaskHubResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.GetInstanceRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.GetInstanceResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.PurgeInstancesRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.PurgeInstancesResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.QueryInstancesRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.QueryInstancesResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.RaiseEventRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.RaiseEventResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ResumeRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ResumeResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.RewindInstanceRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.RewindInstanceResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.SuspendRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.SuspendResponse;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TerminateRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TerminateResponse;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Client-level unit tests for {@link DurableTaskGrpcClient} that verify each method catches
 * {@code StatusRuntimeException} from the sidecar stub and rethrows translated SDK exceptions.
 * <p>
 * Uses gRPC in-process transport with a fake service implementation that throws configured
 * {@code StatusRuntimeException} values.
 */
public class DurableTaskGrpcClientTest {

    private Server inProcessServer;
    private ManagedChannel inProcessChannel;

    @AfterEach
    void tearDown() {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
    }

    /**
     * Creates a {@link DurableTaskGrpcClient} backed by an in-process gRPC server that uses the
     * provided fake service implementation.
     */
    private DurableTaskClient createClientWithFakeService(
            TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase serviceImpl) throws IOException {
        String serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(serviceImpl)
                .build()
                .start();
        inProcessChannel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
        return new DurableTaskGrpcClientBuilder()
                .grpcChannel(inProcessChannel)
                .build();
    }

    /**
     * Asserts that the given exception's cause is a {@link StatusRuntimeException} with the
     * expected status code. gRPC in-process transport recreates exceptions, so we check the
     * status code rather than object identity.
     */
    private static void assertGrpcCause(Throwable ex, Status.Code expectedCode) {
        assertNotNull(ex.getCause(), "Exception should have a cause");
        assertInstanceOf(StatusRuntimeException.class, ex.getCause(),
                "Cause should be StatusRuntimeException but was: " + ex.getCause().getClass().getName());
        StatusRuntimeException cause = (StatusRuntimeException) ex.getCause();
        assertEquals(expectedCode, cause.getStatus().getCode(),
                "Cause status code should be " + expectedCode);
    }

    // -----------------------------------------------------------------------
    // 1. Newly wrapped methods
    // -----------------------------------------------------------------------

    @Test
    void scheduleNewOrchestrationInstance_invalidArgument_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void startInstance(CreateInstanceRequest request, StreamObserver<CreateInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.INVALID_ARGUMENT.withDescription("bad input")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.scheduleNewOrchestrationInstance("TestOrchestrator", new NewOrchestrationInstanceOptions()));

        assertGrpcCause(ex, Status.Code.INVALID_ARGUMENT);
        assertTrue(ex.getMessage().contains("scheduleNewOrchestrationInstance"));
        assertTrue(ex.getMessage().contains("INVALID_ARGUMENT"));
    }

    @Test
    void raiseEvent_notFound_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void raiseEvent(RaiseEventRequest request, StreamObserver<RaiseEventResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("instance not found")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.raiseEvent("test-instance", "testEvent"));

        assertGrpcCause(ex, Status.Code.NOT_FOUND);
        assertTrue(ex.getMessage().contains("raiseEvent"));
    }

    @Test
    void getInstanceMetadata_notFound_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void getInstance(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("instance not found")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.getInstanceMetadata("test-instance", false));

        assertGrpcCause(ex, Status.Code.NOT_FOUND);
    }

    @Test
    void terminate_unavailable_throwsRuntimeException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void terminateInstance(TerminateRequest request, StreamObserver<TerminateResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.UNAVAILABLE.withDescription("connection refused")));
            }
        });

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                client.terminate("test-instance", null));

        assertGrpcCause(ex, Status.Code.UNAVAILABLE);
        assertTrue(ex.getMessage().contains("terminate"));
        assertTrue(ex.getMessage().contains("UNAVAILABLE"));
    }

    @Test
    void queryInstances_unimplemented_throwsUnsupportedOperationException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void queryInstances(QueryInstancesRequest request, StreamObserver<QueryInstancesResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.UNIMPLEMENTED.withDescription("method not supported")));
            }
        });

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class, () ->
                client.queryInstances(new OrchestrationStatusQuery()));

        assertGrpcCause(ex, Status.Code.UNIMPLEMENTED);
        assertTrue(ex.getMessage().contains("queryInstances"));
    }

    @Test
    void createTaskHub_failedPrecondition_throwsIllegalStateException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void createTaskHub(CreateTaskHubRequest request, StreamObserver<CreateTaskHubResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.FAILED_PRECONDITION.withDescription("already exists")));
            }
        });

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                client.createTaskHub(false));

        assertGrpcCause(ex, Status.Code.FAILED_PRECONDITION);
        assertTrue(ex.getMessage().contains("createTaskHub"));
    }

    @Test
    void deleteTaskHub_cancelled_throwsCancellationException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void deleteTaskHub(DeleteTaskHubRequest request, StreamObserver<DeleteTaskHubResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.CANCELLED.withDescription("operation cancelled")));
            }
        });

        CancellationException ex = assertThrows(CancellationException.class, () ->
                client.deleteTaskHub());

        assertGrpcCause(ex, Status.Code.CANCELLED);
    }

    @Test
    void purgeInstance_notFound_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void purgeInstances(PurgeInstancesRequest request, StreamObserver<PurgeInstancesResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("instance not found")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.purgeInstance("test-instance"));

        assertGrpcCause(ex, Status.Code.NOT_FOUND);
    }

    @Test
    void suspendInstance_invalidArgument_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void suspendInstance(SuspendRequest request, StreamObserver<SuspendResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.INVALID_ARGUMENT.withDescription("instanceId is required")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.suspendInstance("test-instance", null));

        assertGrpcCause(ex, Status.Code.INVALID_ARGUMENT);
        assertTrue(ex.getMessage().contains("suspendInstance"));
    }

    @Test
    void resumeInstance_invalidArgument_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void resumeInstance(ResumeRequest request, StreamObserver<ResumeResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.INVALID_ARGUMENT.withDescription("instanceId is required")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.resumeInstance("test-instance", null));

        assertGrpcCause(ex, Status.Code.INVALID_ARGUMENT);
        assertTrue(ex.getMessage().contains("resumeInstance"));
    }

    // -----------------------------------------------------------------------
    // 2. Timeout-declaring methods with DEADLINE_EXCEEDED
    // -----------------------------------------------------------------------

    @Test
    void waitForInstanceStart_deadlineExceeded_throwsTimeoutException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void waitForInstanceStart(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(Status.DEADLINE_EXCEEDED));
            }
        });

        TimeoutException ex = assertThrows(TimeoutException.class, () ->
                client.waitForInstanceStart("test-instance", Duration.ofSeconds(30), false));

        assertNotNull(ex.getMessage());
    }

    @Test
    void waitForInstanceCompletion_deadlineExceeded_throwsTimeoutException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void waitForInstanceCompletion(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(Status.DEADLINE_EXCEEDED));
            }
        });

        TimeoutException ex = assertThrows(TimeoutException.class, () ->
                client.waitForInstanceCompletion("test-instance", Duration.ofSeconds(30), false));

        assertNotNull(ex.getMessage());
    }

    @Test
    void purgeInstances_deadlineExceeded_throwsTimeoutException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void purgeInstances(PurgeInstancesRequest request, StreamObserver<PurgeInstancesResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(Status.DEADLINE_EXCEEDED));
            }
        });

        PurgeInstanceCriteria criteria = new PurgeInstanceCriteria()
                .setCreatedTimeFrom(Instant.parse("2026-01-01T00:00:00Z"));

        TimeoutException ex = assertThrows(TimeoutException.class, () ->
                client.purgeInstances(criteria));

        assertNotNull(ex.getMessage());
    }

    // -----------------------------------------------------------------------
    // 3. Timeout-declaring methods with non-timeout status passthrough
    // -----------------------------------------------------------------------

    @Test
    void waitForInstanceStart_invalidArgument_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void waitForInstanceStart(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.INVALID_ARGUMENT.withDescription("bad instance id")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.waitForInstanceStart("test-instance", Duration.ofSeconds(30), false));

        assertGrpcCause(ex, Status.Code.INVALID_ARGUMENT);
        assertTrue(ex.getMessage().contains("waitForInstanceStart"));
    }

    @Test
    void waitForInstanceCompletion_failedPrecondition_throwsIllegalStateException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void waitForInstanceCompletion(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.FAILED_PRECONDITION.withDescription("not started")));
            }
        });

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                client.waitForInstanceCompletion("test-instance", Duration.ofSeconds(30), false));

        assertGrpcCause(ex, Status.Code.FAILED_PRECONDITION);
        assertTrue(ex.getMessage().contains("waitForInstanceCompletion"));
    }

    @Test
    void purgeInstances_notFound_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void purgeInstances(PurgeInstancesRequest request, StreamObserver<PurgeInstancesResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("no instances match")));
            }
        });

        PurgeInstanceCriteria criteria = new PurgeInstanceCriteria()
                .setCreatedTimeFrom(Instant.parse("2026-01-01T00:00:00Z"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.purgeInstances(criteria));

        assertGrpcCause(ex, Status.Code.NOT_FOUND);
        assertTrue(ex.getMessage().contains("purgeInstances"));
    }

    // -----------------------------------------------------------------------
    // 4. rewindInstance special-case behavior
    // -----------------------------------------------------------------------

    @Test
    void rewindInstance_failedPrecondition_throwsIllegalStateExceptionWithCustomMessage() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void rewindInstance(RewindInstanceRequest request, StreamObserver<RewindInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.FAILED_PRECONDITION.withDescription("not in a failed state")));
            }
        });

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                client.rewindInstance("test-instance", null));

        assertGrpcCause(ex, Status.Code.FAILED_PRECONDITION);
        // rewindInstance has its own custom message for FAILED_PRECONDITION
        assertTrue(ex.getMessage().contains("not in a failed state"));
        assertTrue(ex.getMessage().contains("test-instance"));
    }

    @Test
    void rewindInstance_unavailable_throwsRuntimeExceptionThroughHelper() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void rewindInstance(RewindInstanceRequest request, StreamObserver<RewindInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.UNAVAILABLE.withDescription("connection lost")));
            }
        });

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                client.rewindInstance("test-instance", null));

        assertGrpcCause(ex, Status.Code.UNAVAILABLE);
        assertTrue(ex.getMessage().contains("rewindInstance"));
        assertTrue(ex.getMessage().contains("UNAVAILABLE"));
    }

    @Test
    void rewindInstance_notFound_throwsIllegalArgumentExceptionThroughHelper() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void rewindInstance(RewindInstanceRequest request, StreamObserver<RewindInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("not found")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.rewindInstance("test-instance", null));

        assertGrpcCause(ex, Status.Code.NOT_FOUND);
        // Now goes through the helper, so message contains the operation name
        assertTrue(ex.getMessage().contains("rewindInstance"));
    }

    // -----------------------------------------------------------------------
    // 5. Contract-focused assertions: exception type, cause, message content
    // -----------------------------------------------------------------------

    @Test
    void scheduleNewOrchestrationInstance_messageContainsStatusCodeAndOperationName() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void startInstance(CreateInstanceRequest request, StreamObserver<CreateInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.UNAVAILABLE.withDescription("server down")));
            }
        });

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                client.scheduleNewOrchestrationInstance("TestOrchestrator", new NewOrchestrationInstanceOptions()));

        // Contract: message includes operation name and status code
        assertTrue(ex.getMessage().contains("scheduleNewOrchestrationInstance"),
                "Message should contain operation name but was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("UNAVAILABLE"),
                "Message should contain status code but was: " + ex.getMessage());
        // Contract: original gRPC exception preserved as cause
        assertGrpcCause(ex, Status.Code.UNAVAILABLE);
    }

    @Test
    void getInstanceMetadata_cancelledStatus_throwsCancellationExceptionWithCause() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void getInstance(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(Status.CANCELLED));
            }
        });

        CancellationException ex = assertThrows(CancellationException.class, () ->
                client.getInstanceMetadata("test-instance", false));

        // Contract: cause is preserved for CancellationException
        assertGrpcCause(ex, Status.Code.CANCELLED);
        // Contract: message includes operation name
        assertTrue(ex.getMessage().contains("getInstanceMetadata"));
    }

    @Test
    void suspendInstance_failedPrecondition_fullContractCheck() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void suspendInstance(SuspendRequest request, StreamObserver<SuspendResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.FAILED_PRECONDITION.withDescription("instance already suspended")));
            }
        });

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                client.suspendInstance("test-instance", null));

        // Contract: exception type
        assertInstanceOf(IllegalStateException.class, ex);
        // Contract: cause is original gRPC exception
        assertGrpcCause(ex, Status.Code.FAILED_PRECONDITION);
        // Contract: message contains operation name
        assertTrue(ex.getMessage().contains("suspendInstance"));
        // Contract: message includes status code
        assertTrue(ex.getMessage().contains("FAILED_PRECONDITION"));
    }

    @Test
    void raiseEvent_cancelled_throwsCancellationExceptionWithCause() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void raiseEvent(RaiseEventRequest request, StreamObserver<RaiseEventResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.CANCELLED.withDescription("client cancelled")));
            }
        });

        CancellationException ex = assertThrows(CancellationException.class, () ->
                client.raiseEvent("test-instance", "testEvent"));

        assertGrpcCause(ex, Status.Code.CANCELLED);
    }

    @Test
    void terminate_notFound_throwsIllegalArgumentException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void terminateInstance(TerminateRequest request, StreamObserver<TerminateResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("instance not found")));
            }
        });

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                client.terminate("test-instance", null));

        assertGrpcCause(ex, Status.Code.NOT_FOUND);
        assertTrue(ex.getMessage().contains("terminate"));
        assertTrue(ex.getMessage().contains("NOT_FOUND"));
    }

    @Test
    void deleteTaskHub_unimplemented_throwsUnsupportedOperationException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void deleteTaskHub(DeleteTaskHubRequest request, StreamObserver<DeleteTaskHubResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.UNIMPLEMENTED.withDescription("not supported")));
            }
        });

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class, () ->
                client.deleteTaskHub());

        assertGrpcCause(ex, Status.Code.UNIMPLEMENTED);
        assertTrue(ex.getMessage().contains("deleteTaskHub"));
        assertTrue(ex.getMessage().contains("UNIMPLEMENTED"));
    }

    @Test
    void waitForInstanceStart_cancelled_throwsCancellationException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void waitForInstanceStart(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(Status.CANCELLED));
            }
        });

        CancellationException ex = assertThrows(CancellationException.class, () ->
                client.waitForInstanceStart("test-instance", Duration.ofSeconds(30), false));

        assertGrpcCause(ex, Status.Code.CANCELLED);
    }

    @Test
    void waitForInstanceCompletion_unavailable_throwsRuntimeException() throws IOException {
        DurableTaskClient client = createClientWithFakeService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
            @Override
            public void waitForInstanceCompletion(GetInstanceRequest request, StreamObserver<GetInstanceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(
                        Status.UNAVAILABLE.withDescription("connection refused")));
            }
        });

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                client.waitForInstanceCompletion("test-instance", Duration.ofSeconds(30), false));

        assertGrpcCause(ex, Status.Code.UNAVAILABLE);
        assertTrue(ex.getMessage().contains("waitForInstanceCompletion"));
        assertTrue(ex.getMessage().contains("UNAVAILABLE"));
    }
}
