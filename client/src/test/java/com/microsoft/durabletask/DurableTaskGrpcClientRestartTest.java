// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
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
    private RestartInstanceRequest restartRequest;
    private CreateInstanceRequest createRequest;
    private int metadataRequests;
    private Status restartError;

    private DurableTaskClient startClient() throws Exception {
        String name = InProcessServerBuilder.generateName();
        this.server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                    @Override
                    public void restartInstance(RestartInstanceRequest request,
                                                StreamObserver<RestartInstanceResponse> observer) {
                        restartRequest = request;
                        if (restartError != null) {
                            observer.onError(restartError.asRuntimeException());
                            return;
                        }
                        observer.onNext(RestartInstanceResponse.newBuilder()
                                .setInstanceId(request.getNewInstanceId().getValue()).build());
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
                }).build().start();
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
        assertEquals("order-123-retry", client.restartInstance("order-123", "order-123-retry", version));
        assertEquals("order-123", this.restartRequest.getInstanceId());
        assertTrue(this.restartRequest.getRestartWithNewInstanceId());
        assertTrue(this.restartRequest.hasNewInstanceId());
        assertEquals("order-123-retry", this.restartRequest.getNewInstanceId().getValue());
        assertEquals(version != null, this.restartRequest.hasVersion());
        if (version != null) {
            assertEquals(version, this.restartRequest.getVersion().getValue());
        }
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @Test
    void omittedVersionAndChainedCloneUseBackendRestart() throws Exception {
        DurableTaskClient client = startClient();
        assertEquals("order-123-retry", client.restartInstance("order-123", "order-123-retry"));
        assertFalse(this.restartRequest.hasVersion());
        assertEquals("order-123-retry-again",
                client.restartInstance("order-123-retry", "order-123-retry-again"));
        assertEquals("order-123-retry", this.restartRequest.getInstanceId());
        assertFalse(this.restartRequest.hasVersion());
    }

    @Test
    void explicitTargetMayBeTheSourceId() throws Exception {
        DurableTaskClient client = startClient();
        assertEquals("order-123", client.restartInstance("order-123", "order-123", ""));
        assertEquals("order-123", this.restartRequest.getNewInstanceId().getValue());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void invalidSourceOrTargetIsRejectedBeforeRpc(String id) throws Exception {
        DurableTaskClient client = startClient();
        assertThrows(IllegalArgumentException.class, () -> client.restartInstance("source", id));
        assertThrows(IllegalArgumentException.class, () -> client.restartInstance(id, "target", "2.0"));
        assertNull(this.restartRequest);
        assertEquals(0, this.metadataRequests);
    }

    @ParameterizedTest
    @ValueSource(strings = {"NOT_FOUND", "ALREADY_EXISTS", "UNIMPLEMENTED"})
    void backendErrorsAreNotReplacedWithSchedulingFallback(String status) throws Exception {
        DurableTaskClient client = startClient();
        this.restartError = Status.fromCode(Status.Code.valueOf(status));
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> client.restartInstance("source", "target"));
        assertEquals(this.restartError.getCode(), error.getStatus().getCode());
        assertEquals(0, this.metadataRequests);
        assertNull(this.createRequest);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void legacyRestartStillSchedulesOriginalNameAndInput(boolean newId) throws Exception {
        DurableTaskClient client = startClient();
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
        assertNull(this.restartRequest);
    }
}
