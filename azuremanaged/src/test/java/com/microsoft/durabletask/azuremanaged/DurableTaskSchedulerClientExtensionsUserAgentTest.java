package com.microsoft.durabletask.azuremanaged;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import io.grpc.*;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class DurableTaskSchedulerClientExtensionsUserAgentTest {
    private Server server;
    private Channel channel;
    private final AtomicReference<String> capturedUserAgent = new AtomicReference<>();
    private static final String EXPECTED_USER_AGENT_PREFIX = "durabletask-java/";

    // Dummy gRPC service definition
    public static class DummyService implements io.grpc.BindableService {
        @Override
        public ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder("TaskHubSidecarService")
                .addMethod(
                    MethodDescriptor.<com.google.protobuf.Empty, com.google.protobuf.Empty>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(MethodDescriptor.generateFullMethodName("TaskHubSidecarService", "StartInstance"))
                        .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                        .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()))
                        .build(),
                    ServerCalls.asyncUnaryCall(
                        new ServerCalls.UnaryMethod<com.google.protobuf.Empty, com.google.protobuf.Empty>() {
                            @Override
                            public void invoke(com.google.protobuf.Empty request, StreamObserver<com.google.protobuf.Empty> responseObserver) {
                                // Mock response for StartInstance
                                responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
                                responseObserver.onCompleted();
                            }
                        }
                    )
                )
                .build();
        }
    }

    @BeforeEach
    public void setUp() throws IOException {
        // Use NettyServerBuilder to expose the server via HTTP
        server = NettyServerBuilder.forPort(0)
            .addService(new DummyService()) // Register DummyService
            .intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                    String userAgent = headers.get(Metadata.Key.of("x-user-agent", Metadata.ASCII_STRING_MARSHALLER));
                    capturedUserAgent.set(userAgent);
                    return next.startCall(call, headers);
                }
            })
            .directExecutor()
            .build()
            .start();
        int port = server.getPort();
        String endpoint = "localhost:" + port;
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();
        options.setEndpointAddress(endpoint);
        options.setTaskHubName("testHub");
        options.setAllowInsecureCredentials(true); // Netty is insecure for localhost
        channel = options.createGrpcChannel();
    }

    @AfterEach
    public void tearDown() {
        if (server != null) server.shutdownNow();
        if (channel != null && channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdownNow();
        }
    }

    @Test
    public void testUserAgentHeaderIsSet() {
        DurableTaskGrpcClientBuilder builder = new DurableTaskGrpcClientBuilder();
        builder.grpcChannel(channel);
        DurableTaskClient client = builder.build();

        // Schedule a new orchestration instance
        String instanceId = client.scheduleNewOrchestrationInstance(
            "TestOrchestration",
            new NewOrchestrationInstanceOptions().setInput("TestInput"));

        // Make a dummy call to trigger the request
        String userAgent = capturedUserAgent.get();
        assertNotNull(userAgent, "X-User-Agent header should be set");
        assertTrue(userAgent.startsWith(EXPECTED_USER_AGENT_PREFIX), "X-User-Agent should start with durabletask-java/");
    }
}

