// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ExecutionStartedEvent;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.HistoryEvent;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.OrchestrationInstance;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.OrchestratorStartedEvent;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TraceContext;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link TaskOrchestrationExecutor} emits the orchestration-initiated entity signal
 * PRODUCER span when {@code emitTraceSpans} is enabled (standalone/DTS worker) and suppresses it
 * otherwise (Azure Functions, where the host emits the entity spans).
 */
public class TaskOrchestrationEntityTracingTest {

    private static final Logger logger = Logger.getLogger(TaskOrchestrationEntityTracingTest.class.getName());
    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String ORCH_SPAN_ID = "b7ad6b7169203331";

    private InMemorySpanExporter spanExporter;
    private OpenTelemetrySdk openTelemetry;

    @BeforeEach
    void setUp() {
        io.opentelemetry.api.GlobalOpenTelemetry.resetForTest();
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    @AfterEach
    void tearDown() {
        openTelemetry.close();
        io.opentelemetry.api.GlobalOpenTelemetry.resetForTest();
    }

    private TaskOrchestrationExecutor createExecutor(
            String orchestratorName, TaskOrchestration orchestration, boolean emitTraceSpans) {
        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchestratorName, new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return orchestratorName;
            }

            @Override
            public TaskOrchestration create() {
                return orchestration;
            }
        });
        return new TaskOrchestrationExecutor(
                factories,
                new JacksonDataConverter(),
                Duration.ofDays(1),
                logger,
                null,
                true,
                null,
                emitTraceSpans);
    }

    private HistoryEvent orchestratorStarted() {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                .build();
    }

    private HistoryEvent executionStarted(String name) {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setName(name)
                        .setVersion(StringValue.of(""))
                        .setInput(StringValue.of("null"))
                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                .setInstanceId("test-instance-id")
                                .build())
                        .build())
                .build();
    }

    private static TraceContext orchestrationContext() {
        return TraceContext.newBuilder()
                .setTraceParent("00-" + TRACE_ID + "-" + ORCH_SPAN_ID + "-01")
                .build();
    }

    @Test
    void signalEntity_emitsProducerSpanUnderOrchestrationContext() {
        String orchestratorName = "SignalOrch";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.signalEntity(entityId, "add", 5);
            ctx.complete("done");
        }, true);

        executor.execute(
                Collections.emptyList(),
                Arrays.asList(orchestratorStarted(), executionStarted(orchestratorName)),
                orchestrationContext());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData producer = spans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER).findFirst().orElse(null);
        assertNotNull(producer, "expected PRODUCER signal span");
        assertEquals("entity:counter:add", producer.getName());
        assertEquals("signal_entity",
                producer.getAttributes().get(AttributeKey.stringKey("durabletask.task.operation")));
        assertEquals(TRACE_ID, producer.getTraceId());
        assertEquals(ORCH_SPAN_ID, producer.getParentSpanId());
    }

    @Test
    void signalEntity_emitTraceSpansDisabled_suppressesProducerSpan() {
        String orchestratorName = "SignalOrchDisabled";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.signalEntity(entityId, "add", 5);
            ctx.complete("done");
        }, false);

        executor.execute(
                Collections.emptyList(),
                Arrays.asList(orchestratorStarted(), executionStarted(orchestratorName)),
                orchestrationContext());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertTrue(spans.stream().noneMatch(s -> s.getKind() == SpanKind.PRODUCER),
                "expected no PRODUCER span when emitTraceSpans is disabled");
    }
}
