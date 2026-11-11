// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class DurableTaskGrpcClientRestartTest {
    private Server server;
    private ManagedChannel channel;
    private CloneInstanceRequest cloneRequest;
    private RestartInstanceRequest restartRequest;
    private CreateInstanceRequest createRequest;
    private int metadataRequests;
    private Status cloneError;

    private DurableTaskClient startClient() throws Exception {
        return startClient(true);
    }

    private DurableTaskClient startClient(boolean supportsCloning) throws Exception {
        String name = InProcessServerBuilder.generateName();
        TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase service =
                new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                    @Override
                    public void cloneInstance(CloneInstanceRequest request,
                                              StreamObserver<CloneInstanceResponse> observer) {
                        cloneRequest = request;
                        if (cloneError != null) {
                            observer.onError(cloneError.asRuntimeException());
                            return;
                        }
                        observer.onNext(CloneInstanceResponse.newBuilder()
                                .setInstanceId(request.getNewInstanceId()).build());
                        observer.onCompleted();
                    }

                    @Override
                    public void restartInstance(RestartInstanceRequest request,
                                                StreamObserver<RestartInstanceResponse> observer) {
                        restartRequest = request;
                        observer.onNext(RestartInstanceResponse.newBuilder()
                                .setInstanceId("legacy-restart").build());
                        observer.onCompleted();
                    }

                    @Override
                    public void getInstance(GetInstanceRequest request, StreamObserver<GetInstanceResponse> observer) {
                        metadataRequests++;
                        observer.onNext(GetInstanceResponse.newBuilder().setExists(true)
                                .setOrchestrationState(OrchestrationState.newBuilder()
                                        .setInstanceId(request.getInstanceId())
                                        .setName("Order")
                                        .setInput(StringValue.of("{\"order\":123}"))).build());
                        observer.onCompleted();
                    }

                    @Override
                    public void startInstance(CreateInstanceRequest request,
                                              StreamObserver<CreateInstanceResponse> observer) {
                        createRequest = request;
                        observer.onNext(CreateInstanceResponse.newBuilder()
                                .setInstanceId(request.getInstanceId()).build());
                        observer.onCompleted();
                    }
                };
        ServerServiceDefinition.Builder serviceDefinition =
                ServerServiceDefinition.builder(TaskHubSidecarServiceGrpc.SERVICE_NAME);
        for (ServerMethodDefinition<?, ?> method : service.bindService().getMethods()) {
            if (supportsCloning || !method.getMethodDescriptor().getFullMethodName()
                    .equals(TaskHubSidecarServiceGrpc.getCloneInstanceMethod().getFullMethodName())) {
                serviceDefinition.addMethod(method);
            }
        }
        this.server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(serviceDefinition.build()).build().start();
        this.channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return new DurableTaskGrpcClientBuilder().grpcChannel(this.channel).defaultVersion("client-default").build();
    }

    @AfterEach
    void tearDown() {
        if (this.channel != null) {
            this.channel.shutdownNow();
        }
        if (this.server != null) {
            this.server.shutdownNow();
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"2.0"})
    void explicitTargetPreservesVersionPresence(String version) throws Exception {
        DurableTaskClient client = startClient();
        assertEquals("order-123-retry", client.restartInstance("order-123",
                new RestartInstanceOptions().setNewInstanceId("order-123-retry").setVersion(version)));
        assertEquals("order-123", this.cloneRequest.getSourceInstanceId());
        assertEquals("order-123-retry", this.cloneRequest.getNewInstanceId());
        assertEquals(version != null, this.cloneRequest.hasVersion());
        if (version != null) {
            assertEquals(version, this.cloneRequest.getVersion().getValue());
        }
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @Test
    void omittedVersionAndChainedCloneUseCloneRpc() throws Exception {
        DurableTaskClient client = startClient();
        assertEquals("order-123-retry", client.restartInstance("order-123",
                new RestartInstanceOptions().setNewInstanceId("order-123-retry")));
        assertFalse(this.cloneRequest.hasVersion());
        assertEquals("order-123-retry-again",
                client.restartInstance("order-123-retry",
                        new RestartInstanceOptions().setNewInstanceId("order-123-retry-again")));
        assertEquals("order-123-retry", this.cloneRequest.getSourceInstanceId());
        assertEquals("order-123-retry-again", this.cloneRequest.getNewInstanceId());
        assertFalse(this.cloneRequest.hasVersion());
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @Test
    void explicitTargetMayBeTheSourceId() throws Exception {
        DurableTaskClient client = startClient();
        assertEquals("order-123", client.restartInstance("order-123",
                new RestartInstanceOptions().setNewInstanceId("order-123").setVersion("")));
        assertEquals("order-123", this.cloneRequest.getSourceInstanceId());
        assertEquals("order-123", this.cloneRequest.getNewInstanceId());
        assertTrue(this.cloneRequest.hasVersion());
        assertEquals("", this.cloneRequest.getVersion().getValue());
        assertNull(this.restartRequest);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void invalidSourceOrTargetIsRejectedBeforeRpc(String id) throws Exception {
        DurableTaskClient client = startClient();
        assertThrows(IllegalArgumentException.class, () -> client.restartInstance("source",
                new RestartInstanceOptions().setNewInstanceId(id)));
        assertThrows(IllegalArgumentException.class, () -> client.restartInstance(id,
                new RestartInstanceOptions().setNewInstanceId("target").setVersion("2.0")));
        assertNull(this.cloneRequest);
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @Test
    void missingOptionsOrTargetIsRejectedBeforeRpc() throws Exception {
        DurableTaskClient client = startClient();
        assertThrows(IllegalArgumentException.class, () -> client.restartInstance("source", null));
        assertThrows(IllegalArgumentException.class,
                () -> client.restartInstance("source", new RestartInstanceOptions()));
        assertNull(this.cloneRequest);
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID_ARGUMENT", "NOT_FOUND", "ALREADY_EXISTS", "UNIMPLEMENTED"})
    void backendErrorsAreNotReplacedWithSchedulingFallback(String status) throws Exception {
        DurableTaskClient client = startClient();
        this.cloneError = Status.fromCode(Status.Code.valueOf(status));
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> client.restartInstance("source", new RestartInstanceOptions().setNewInstanceId("target")));
        assertEquals(this.cloneError.getCode(), error.getStatus().getCode());
        assertNotNull(this.cloneRequest);
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"2.0"})
    void unsupportedServerRejectsCloneWithoutCallingLegacyRestart(String version) throws Exception {
        DurableTaskClient client = startClient(false);
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> client.restartInstance("source",
                        new RestartInstanceOptions().setNewInstanceId("target").setVersion(version)));
        assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
        assertNull(this.cloneRequest);
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void legacyRestartStillSchedulesOriginalNameAndInput(boolean newId) throws Exception {
        DurableTaskClient client = startClient(false);
        String result = client.restartInstance("order-123", newId);
        assertEquals(this.createRequest.getInstanceId(), result);
        if (newId) {
            assertNotEquals("order-123", result);
        } else {
            assertEquals("order-123", result);
        }
        assertEquals("Order", this.createRequest.getName());
        assertEquals("{\"order\":123}", this.createRequest.getInput().getValue());
        assertEquals("client-default", this.createRequest.getVersion().getValue());
        assertEquals(1, this.metadataRequests);
        assertNull(this.cloneRequest);
        assertNull(this.restartRequest);
    }
}
