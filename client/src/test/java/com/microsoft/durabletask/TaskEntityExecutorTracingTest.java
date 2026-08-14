// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityBatchRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityBatchResult;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.OperationRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TraceContext;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link TaskEntityExecutor} emits the entity processing (SERVER) span when
 * {@code emitTraceSpans} is enabled and suppresses it otherwise.
 */
public class TaskEntityExecutorTracingTest {

    private static final Logger logger = Logger.getLogger(TaskEntityExecutorTracingTest.class.getName());
    private static final DataConverter dataConverter = new JacksonDataConverter();
    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String PARENT_SPAN_ID = "b7ad6b7169203331";

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

    /** Minimal counter entity used for the batch under test. */
    static class CounterEntity extends AbstractTaskEntity<Integer> {
        public void add(int amount) {
            this.state += amount;
        }

        public void signalOther(int amount) {
            this.context.signalEntity(new EntityInstanceId("counter", "c2"), "add", amount);
        }

        public void startOrch() {
            this.context.startNewOrchestration("DownstreamOrch", null);
        }

        @Override
        protected Integer initializeState(TaskEntityOperation operation) {
            return 0;
        }

        @Override
        protected Class<Integer> getStateType() {
            return Integer.class;
        }
    }

    private TaskEntityExecutor createExecutor(boolean emitTraceSpans) {
        HashMap<String, TaskEntityFactory> factories = new HashMap<>();
        factories.put("counter", CounterEntity::new);
        return new TaskEntityExecutor(factories, dataConverter, logger, emitTraceSpans);
    }

    private EntityBatchRequest requestWith(@javax.annotation.Nullable TraceContext traceContext) {
        return requestWithOp("add", 5, traceContext);
    }

    private EntityBatchRequest requestWithOp(
            String operation, int input, @javax.annotation.Nullable TraceContext traceContext) {
        OperationRequest.Builder op = OperationRequest.newBuilder()
                .setOperation(operation)
                .setRequestId("req-1")
                .setInput(StringValue.of(dataConverter.serialize(input)));
        if (traceContext != null) {
            op.setTraceContext(traceContext);
        }
        return EntityBatchRequest.newBuilder()
                .setInstanceId("@counter@c1")
                .setEntityState(StringValue.of(dataConverter.serialize(10)))
                .addOperations(op.build())
                .build();
    }

    private static TraceContext parentTraceContext() {
        return TraceContext.newBuilder()
                .setTraceParent("00-" + TRACE_ID + "-" + PARENT_SPAN_ID + "-01")
                .build();
    }

    @Test
    void execute_emitsEntityProcessingServerSpanUnderParent() {
        TaskEntityExecutor executor = createExecutor(true);

        EntityBatchResult result = executor.execute(requestWith(parentTraceContext()));
        assertTrue(result.getResults(0).hasSuccess());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData span = spans.get(0);
        assertEquals("entity:counter:add", span.getName());
        assertEquals(SpanKind.SERVER, span.getKind());
        assertEquals(TRACE_ID, span.getTraceId());
        assertEquals(PARENT_SPAN_ID, span.getParentSpanId());
        assertEquals("entity", span.getAttributes().get(AttributeKey.stringKey("durabletask.type")));
        assertEquals("call_entity",
                span.getAttributes().get(AttributeKey.stringKey("durabletask.task.operation")));
        assertEquals("@counter@c1",
                span.getAttributes().get(AttributeKey.stringKey("durabletask.task.instance_id")));
        assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
    }

    @Test
    void execute_emitTraceSpansDisabled_suppressesSpan() {
        TaskEntityExecutor executor = createExecutor(false);

        EntityBatchResult result = executor.execute(requestWith(parentTraceContext()));
        assertTrue(result.getResults(0).hasSuccess());

        assertTrue(spanExporter.getFinishedSpanItems().isEmpty());
    }

    @Test
    void execute_noParentTraceContext_emitsNoSpan() {
        TaskEntityExecutor executor = createExecutor(true);

        EntityBatchResult result = executor.execute(requestWith(null));
        assertTrue(result.getResults(0).hasSuccess());

        assertTrue(spanExporter.getFinishedSpanItems().isEmpty());
    }

    @Test
    void execute_entitySignalsEntity_emitsProducerSpanNestedUnderProcessingSpan() {
        TaskEntityExecutor executor = createExecutor(true);

        EntityBatchResult result = executor.execute(requestWithOp("signalOther", 3, parentTraceContext()));
        assertTrue(result.getResults(0).hasSuccess());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData server = spans.stream().filter(s -> s.getKind() == SpanKind.SERVER).findFirst().orElse(null);
        SpanData producer = spans.stream().filter(s -> s.getKind() == SpanKind.PRODUCER).findFirst().orElse(null);
        assertNotNull(server, "expected SERVER processing span");
        assertNotNull(producer, "expected PRODUCER signal span");
        assertEquals("entity:counter:add", producer.getName());
        assertEquals("signal_entity",
                producer.getAttributes().get(AttributeKey.stringKey("durabletask.task.operation")));
        assertEquals(TRACE_ID, producer.getTraceId());
        assertEquals(server.getSpanId(), producer.getParentSpanId());
    }

    @Test
    void execute_entityStartsOrchestration_emitsProducerSpanNestedUnderProcessingSpan() {
        TaskEntityExecutor executor = createExecutor(true);

        EntityBatchResult result = executor.execute(requestWithOp("startOrch", 0, parentTraceContext()));
        assertTrue(result.getResults(0).hasSuccess());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData server = spans.stream().filter(s -> s.getKind() == SpanKind.SERVER).findFirst().orElse(null);
        SpanData producer = spans.stream().filter(s -> s.getKind() == SpanKind.PRODUCER).findFirst().orElse(null);
        assertNotNull(server, "expected SERVER processing span");
        assertNotNull(producer, "expected PRODUCER create_orchestration span");
        assertEquals("counter:create_orchestration", producer.getName());
        assertEquals("entity", producer.getAttributes().get(AttributeKey.stringKey("durabletask.type")));
        assertEquals(TRACE_ID, producer.getTraceId());
        assertEquals(server.getSpanId(), producer.getParentSpanId());
    }
}
