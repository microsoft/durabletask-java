// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.google.protobuf.Empty;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcWorker;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc.TaskHubSidecarServiceBlockingStub;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AudienceTransportTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void localGrpcReconnectRetainsScopeTokenCacheAndWorkerId(boolean worker) throws Exception {
        List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
        List<String> workerIds = Collections.synchronizedList(new ArrayList<>());
        Set<SocketAddress> connections = Collections.synchronizedSet(new HashSet<>());
        Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(ServerServiceDefinition.builder("test.Service")
                .addMethod(ResourceIdTest.METHOD, ServerCalls.asyncUnaryCall((request, response) -> {
                    response.onNext(Empty.getDefaultInstance());
                    response.onCompleted();
                }))
                .build())
            .intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                    authorizations.add(headers.get(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)));
                    workerIds.add(headers.get(Metadata.Key.of("workerid", Metadata.ASCII_STRING_MARSHALLER)));
                    connections.add(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
                    return next.startCall(call, headers);
                }
            })
            .build().start();
        ManagedChannel channel = null;
        DurableTaskGrpcWorker taskWorker = null;
        try (MockedStatic<ResourceId> defaults = mockStatic(ResourceId.class, CALLS_REAL_METHODS)) {
            defaults.when(ResourceId::getDefault).thenReturn("https://durabletask.azure.us");
            ResourceIdTest.RecordingCredential credential = new ResourceIdTest.RecordingCredential();
            String endpoint = "http://127.0.0.1:" + server.getPort();
            channel = (ManagedChannel) (worker
                ? new DurableTaskSchedulerWorkerOptions().setEndpointAddress(endpoint)
                    .setAllowInsecureCredentials(true).setTaskHubName("test-hub")
                    .setCredential(credential).createGrpcChannel()
                : new DurableTaskSchedulerClientOptions().setEndpointAddress(endpoint)
                    .setAllowInsecureCredentials(true).setTaskHubName("test-hub")
                    .setCredential(credential).createGrpcChannel());
            Channel requestChannel = channel;
            if (worker) {
                taskWorker = new DurableTaskGrpcWorkerBuilder().grpcChannel(channel).build();
                requestChannel = workerChannel(taskWorker);
            }
            assertTrue(credential.scopes.isEmpty());
            call(requestChannel);
            defaults.when(ResourceId::getDefault).thenReturn("https://durabletask.io");
            channel.enterIdle();
            call(requestChannel);
            call(requestChannel);
            assertEquals(Arrays.asList("Bearer token-1", "Bearer token-2", "Bearer token-2"), authorizations);
            assertEquals(Collections.nCopies(2, "https://durabletask.azure.us/.default"), credential.scopes);
            assertEquals(2, connections.size(), "The second call must use a new transport connection");
            if (worker) {
                assertNotNull(workerIds.get(0));
                assertFalse(workerIds.get(0).isEmpty());
                assertEquals(Collections.nCopies(3, workerIds.get(0)), workerIds,
                    "The worker ID must survive token refresh and transport reconnection");
            } else {
                assertEquals(Collections.nCopies(3, null), workerIds);
            }
        } finally {
            if (taskWorker != null) {
                taskWorker.close();
            }
            if (channel != null) {
                channel.shutdownNow();
            }
            server.shutdownNow();
            if (channel != null) {
                assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
            }
            assertTrue(server.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void invalidWorkerProcessIdFailsWithAnExplicitError() {
        RuntimeMXBean runtime = mock(RuntimeMXBean.class);
        when(runtime.getName()).thenReturn("not-a-pid@host");
        try (MockedStatic<ManagementFactory> management = mockStatic(ManagementFactory.class)) {
            management.when(ManagementFactory::getRuntimeMXBean).thenReturn(runtime);
            IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new DurableTaskGrpcWorkerBuilder().build());
            assertEquals("Cannot determine the worker process ID.", error.getMessage());
            assertInstanceOf(NumberFormatException.class, error.getCause());
        }
    }

    @Test
    void callerSuppliedChannelsRemainCallerOwnedAndDoNotAcquireTokensAtBuildTime() {
        ResourceIdTest.RecordingCredential credential = new ResourceIdTest.RecordingCredential();
        ManagedChannel suppliedChannel = mock(ManagedChannel.class);
        try (ResourceIdTest.ChannelCapture channels = new ResourceIdTest.ChannelCapture();
             DurableTaskClient client = DurableTaskSchedulerClientExtensions
                .createClientBuilder("https://scheduler.example", "test-hub", credential, "api://Custom")
                .grpcChannel(suppliedChannel).build();
             DurableTaskGrpcWorker worker = DurableTaskSchedulerWorkerExtensions
                .createWorkerBuilder("https://scheduler.example", "test-hub", credential, "api://Custom")
                .grpcChannel(suppliedChannel).build()) {
            assertTrue(credential.scopes.isEmpty());
        }
        verify(suppliedChannel, never()).shutdown();
        verify(suppliedChannel, never()).shutdownNow();
        assertTrue(credential.scopes.isEmpty());
    }

    private static Channel workerChannel(DurableTaskGrpcWorker worker) throws ReflectiveOperationException {
        Field field = DurableTaskGrpcWorker.class.getDeclaredField("sidecarClient");
        field.setAccessible(true);
        return ((TaskHubSidecarServiceBlockingStub) field.get(worker)).getChannel();
    }

    private static void call(Channel channel) {
        ClientCalls.blockingUnaryCall(channel, ResourceIdTest.METHOD,
            CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS), Empty.getDefaultInstance());
    }
}
