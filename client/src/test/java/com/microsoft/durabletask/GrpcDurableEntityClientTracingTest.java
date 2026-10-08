// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.SignalEntityRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.SignalEntityResponse;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GrpcDurableEntityClientTracingTest {

    private final AtomicReference<SignalEntityRequest> capturedRequest = new AtomicReference<>();
    private InMemorySpanExporter spanExporter;
    private OpenTelemetrySdk openTelemetry;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private DurableTaskClient client;

    @BeforeEach
    void setUp() throws Exception {
        GlobalOpenTelemetry.resetForTest();
        this.spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(this.spanExporter))
                .build();
        this.openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();

        String serverName = InProcessServerBuilder.generateName();
        this.inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                    @Override
                    public void signalEntity(
                            SignalEntityRequest request,
                            StreamObserver<SignalEntityResponse> responseObserver) {
                        capturedRequest.set(request);
                        responseObserver.onNext(SignalEntityResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        this.inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        this.client = new DurableTaskGrpcClientBuilder().grpcChannel(this.inProcessChannel).build();
    }

    @AfterEach
    void tearDown() {
        if (this.inProcessChannel != null) {
            this.inProcessChannel.shutdownNow();
        }
        if (this.inProcessServer != null) {
            this.inProcessServer.shutdownNow();
        }
        if (this.openTelemetry != null) {
            this.openTelemetry.close();
        }
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void signalEntity_emitsProducerSpanAndPropagatesItsContext() {
        Span parentSpan = GlobalOpenTelemetry.getTracer("test").spanBuilder("parent").startSpan();
        try (Scope ignored = parentSpan.makeCurrent()) {
            this.client.getEntities().signalEntity(
                    new EntityInstanceId("Counter", "c1"),
                    "add",
                    5);
        } finally {
            parentSpan.end();
        }

        SignalEntityRequest request = this.capturedRequest.get();
        assertNotNull(request);
        assertTrue(request.hasRequestTime());
        assertTrue(request.getRequestTime().getSeconds() > 0);

        SpanData producer = this.spanExporter.getFinishedSpanItems().stream()
                .filter(span -> span.getKind() == SpanKind.PRODUCER)
                .findFirst()
                .orElse(null);
        assertNotNull(producer, "expected external entity signal PRODUCER span");
        assertEquals("entity:counter:add", producer.getName());
        assertEquals(parentSpan.getSpanContext().getSpanId(), producer.getParentSpanId());
        assertEquals(producer.getSpanId(),
                request.getParentTraceContext().getTraceParent().split("-")[2]);
    }
}