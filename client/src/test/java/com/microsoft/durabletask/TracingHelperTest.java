// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TraceContext;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TracingHelper.
 */
public class TracingHelperTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private OpenTelemetrySdk openTelemetry;

    @BeforeEach
    void setUp() {
        // Reset first in case another test class triggered GlobalOpenTelemetry.get()
        io.opentelemetry.api.GlobalOpenTelemetry.resetForTest();
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    @AfterEach
    void tearDown() {
        openTelemetry.close();
        // Reset the global OpenTelemetry to avoid affecting other tests
        io.opentelemetry.api.GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void getCurrentTraceContext_noActiveSpan_returnsNull() {
        TraceContext result = TracingHelper.getCurrentTraceContext();
        assertNull(result);
    }

    @Test
    void getCurrentTraceContext_withActiveSpan_returnsTraceContext() {
        Tracer tracer = openTelemetry.getTracer("test");
        Span span = tracer.spanBuilder("test-span").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            TraceContext result = TracingHelper.getCurrentTraceContext();
            assertNotNull(result);
            assertNotNull(result.getTraceParent());
            assertTrue(result.getTraceParent().startsWith("00-"));

            // traceparent format: 00-<traceId>-<spanId>-<flags>
            String[] parts = result.getTraceParent().split("-");
            assertEquals(4, parts.length);
            assertEquals(32, parts[1].length()); // trace ID
            assertEquals(16, parts[2].length()); // span ID
        } finally {
            span.end();
        }
    }

    @Test
    void extractTraceContext_null_returnsNull() {
        Context result = TracingHelper.extractTraceContext(null);
        assertNull(result);
    }

    @Test
    void extractTraceContext_emptyTraceParent_returnsNull() {
        TraceContext emptyCtx = TraceContext.newBuilder().build();
        Context result = TracingHelper.extractTraceContext(emptyCtx);
        assertNull(result);
    }

    @Test
    void extractTraceContext_validTraceParent_returnsContext() {
        TraceContext protoCtx = TraceContext.newBuilder()
                .setTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                .build();

        Context result = TracingHelper.extractTraceContext(protoCtx);
        assertNotNull(result);

        // Verify we can extract the span context from the OTel context
        Span span = Span.fromContext(result);
        assertTrue(span.getSpanContext().isValid());
        assertEquals("0af7651916cd43dd8448eb211c80319c", span.getSpanContext().getTraceId());
        assertEquals("b7ad6b7169203331", span.getSpanContext().getSpanId());
    }

    @Test
    void extractTraceContext_withTraceState_preservesState() {
        TraceContext protoCtx = TraceContext.newBuilder()
                .setTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                .setTraceState(StringValue.of("vendorname=opaqueValue"))
                .build();

        Context result = TracingHelper.extractTraceContext(protoCtx);
        assertNotNull(result);

        Span span = Span.fromContext(result);
        assertEquals("opaqueValue", span.getSpanContext().getTraceState().get("vendorname"));
    }

    @Test
    void startSpan_createsSpanWithAttributes() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(TracingHelper.ATTR_TYPE, TracingHelper.TYPE_ACTIVITY);
        attrs.put(TracingHelper.ATTR_TASK_NAME, "test-activity");
        attrs.put(TracingHelper.ATTR_INSTANCE_ID, "abc123");

        Span span = TracingHelper.startSpan("activity:test-activity", null, SpanKind.SERVER, attrs);
        assertNotNull(span);
        span.end();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData spanData = spans.get(0);
        assertEquals("activity:test-activity", spanData.getName());
        assertEquals(io.opentelemetry.api.trace.SpanKind.SERVER, spanData.getKind());
        assertEquals("test-activity", spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.name")));
        assertEquals("abc123", spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.instance_id")));
        assertEquals("activity", spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
    }

    @Test
    void startSpan_withParentTraceContext_createsChildSpan() {
        // Create a parent span first
        Tracer tracer = openTelemetry.getTracer("test");
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        TraceContext parentCtx;
        try (Scope ignored = parentSpan.makeCurrent()) {
            parentCtx = TracingHelper.getCurrentTraceContext();
        } finally {
            parentSpan.end();
        }

        assertNotNull(parentCtx);

        // Create a child span using the trace context
        Span childSpan = TracingHelper.startSpan("child", parentCtx, SpanKind.INTERNAL, null);
        childSpan.end();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(2, spans.size());

        // Find parent and child spans
        SpanData parentData = spans.stream()
                .filter(s -> s.getName().equals("parent"))
                .findFirst().orElseThrow();
        SpanData childData = spans.stream()
                .filter(s -> s.getName().equals("child"))
                .findFirst().orElseThrow();

        // Verify child has same trace ID as parent
        assertEquals(parentData.getTraceId(), childData.getTraceId());
        // Verify child's parent span ID matches the parent span
        assertEquals(parentData.getSpanId(), childData.getParentSpanId());
    }

    @Test
    void endSpan_withError_recordsException() {
        Span span = TracingHelper.startSpan("error-span", null, SpanKind.INTERNAL, null);
        RuntimeException error = new RuntimeException("test error");
        TracingHelper.endSpan(span, error);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData spanData = spans.get(0);
        assertEquals(io.opentelemetry.api.trace.StatusCode.ERROR, spanData.getStatus().getStatusCode());
        assertFalse(spanData.getEvents().isEmpty(), "Should have recorded exception event");
    }

    @Test
    void endSpan_withNullSpan_doesNotThrow() {
        assertDoesNotThrow(() -> TracingHelper.endSpan(null, null));
        assertDoesNotThrow(() -> TracingHelper.endSpan(null, new RuntimeException("test")));
    }

    @Test
    void endSpan_withoutError_endsCleanly() {
        Span span = TracingHelper.startSpan("clean-span", null, SpanKind.INTERNAL, null);
        TracingHelper.endSpan(span, null);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        assertEquals(io.opentelemetry.api.trace.StatusCode.UNSET, spans.get(0).getStatus().getStatusCode());
    }

    @Test
    void getCurrentTraceContext_roundTrip() {
        // Create a span, capture trace context, extract it, create child - verify full round trip
        Tracer tracer = openTelemetry.getTracer("test");
        Span originalSpan = tracer.spanBuilder("original").startSpan();
        TraceContext captured;
        try (Scope ignored = originalSpan.makeCurrent()) {
            captured = TracingHelper.getCurrentTraceContext();
        } finally {
            originalSpan.end();
        }

        assertNotNull(captured);

        // Extract back to OTel context
        Context extractedCtx = TracingHelper.extractTraceContext(captured);
        assertNotNull(extractedCtx);

        // Verify the extracted context matches the original span
        Span extractedSpan = Span.fromContext(extractedCtx);
        assertEquals(originalSpan.getSpanContext().getTraceId(),
                extractedSpan.getSpanContext().getTraceId());
        assertEquals(originalSpan.getSpanContext().getSpanId(),
                extractedSpan.getSpanContext().getSpanId());
    }

    @Test
    void emitTimerSpan_createsInternalSpanWithFireAt() {
        TracingHelper.emitTimerSpan("MyOrchestration", "instance-1", 5, "2026-01-01T00:00:00Z", null, null);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("orchestration:MyOrchestration:timer", sd.getName());
        assertEquals(SpanKind.INTERNAL, sd.getKind());
        assertEquals("timer", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
        assertEquals("MyOrchestration", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.name")));
        assertEquals("instance-1", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.instance_id")));
        assertEquals("5", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.task_id")));
        assertEquals("2026-01-01T00:00:00Z", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.fire_at")));
    }

    @Test
    void emitTimerSpan_withStartTime_setsStartTimestamp() {
        java.time.Instant startTime = java.time.Instant.parse("2026-01-01T00:00:00Z");
        TracingHelper.emitTimerSpan("MyOrchestration", "instance-1", 5, "2026-01-01T00:01:00Z", null, startTime);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("orchestration:MyOrchestration:timer", sd.getName());
        // The start time should be set to the provided startTime
        long startEpochNanos = startTime.getEpochSecond() * 1_000_000_000L + startTime.getNano();
        assertEquals(startEpochNanos, sd.getStartEpochNanos());
    }

    @Test
    void emitRetroactiveClientSpan_createsClientSpanWithStartTime() {
        java.time.Instant startTime = java.time.Instant.parse("2026-01-01T00:00:00Z");
        TracingHelper.emitRetroactiveClientSpan(
                "activity:GetWeather", null, TracingHelper.TYPE_ACTIVITY,
                "GetWeather", "instance-1", 3, startTime, null);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("activity:GetWeather", sd.getName());
        assertEquals(SpanKind.CLIENT, sd.getKind());
        assertEquals("activity", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
        assertEquals("GetWeather", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.name")));
        assertEquals("instance-1", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.instance_id")));
        assertEquals("3", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.task_id")));
        long startEpochNanos = startTime.getEpochSecond() * 1_000_000_000L + startTime.getNano();
        assertEquals(startEpochNanos, sd.getStartEpochNanos());
    }

    @Test
    void emitRetroactiveClientSpan_setsSpanId_whenProvided() {
        String targetSpanId = "abcdef1234567890";
        TracingHelper.emitRetroactiveClientSpan(
                "activity:GetWeather", null, TracingHelper.TYPE_ACTIVITY,
                "GetWeather", "instance-1", 3, null, targetSpanId);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        assertEquals(targetSpanId, spans.get(0).getSpanContext().getSpanId());
    }

    @Test
    void setSpanId_changesSpanId() {
        Tracer tracer = openTelemetry.getTracer("test");
        Span span = tracer.spanBuilder("test-span").startSpan();
        String originalId = span.getSpanContext().getSpanId();
        String targetId = "abcdef1234567890";

        TracingHelper.setSpanId(span, targetId);

        assertEquals(targetId, span.getSpanContext().getSpanId());
        assertNotEquals(originalId, targetId);
        span.end();
    }

    @Test
    void setSpanId_nullSpan_doesNotThrow() {
        TracingHelper.setSpanId(null, "abcdef1234567890");
    }

    @Test
    void extractSpanIdFromTraceparent_validTraceparent() {
        assertEquals("1234567890abcdef",
                TracingHelper.extractSpanIdFromTraceparent("00-0af7651916cd43dd8448eb211c80319c-1234567890abcdef-01"));
    }

    @Test
    void extractSpanIdFromTraceparent_null_returnsNull() {
        assertNull(TracingHelper.extractSpanIdFromTraceparent(null));
    }

    @Test
    void emitEventSpan_fromWorker_createsProducerSpan() {
        TracingHelper.emitEventSpan("ApprovalEvent", "orch-1", "target-orch-2");

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("orchestration_event:ApprovalEvent", sd.getName());
        assertEquals(SpanKind.PRODUCER, sd.getKind());
        assertEquals("event", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
        assertEquals("ApprovalEvent", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.name")));
        assertEquals("orch-1", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.instance_id")));
        assertEquals("target-orch-2", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.event.target_instance_id")));
    }

    @Test
    void emitEventSpan_fromClient_createsProducerSpan() {
        TracingHelper.emitEventSpan("ApprovalEvent", null, "target-orch-1");

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("orchestration_event:ApprovalEvent", sd.getName());
        assertEquals(SpanKind.PRODUCER, sd.getKind());
        assertEquals("event", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
        assertEquals("target-orch-1", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.event.target_instance_id")));
    }

    // region Entity spans

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String PARENT_SPAN_ID = "b7ad6b7169203331";

    private static TraceContext parentCtx() {
        return TraceContext.newBuilder()
                .setTraceParent("00-" + TRACE_ID + "-" + PARENT_SPAN_ID + "-01")
                .build();
    }

    private static String attr(SpanData sd, String key) {
        return sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(key));
    }

    @Test
    void createEntitySpanName_usesEntityAndOperation() {
        assertEquals("entity:Counter:add", TracingHelper.createEntitySpanName("Counter", "add"));
    }

    @Test
    void createEntityStartOrchestrationSpanName_isInverted() {
        assertEquals("Counter:create_orchestration",
                TracingHelper.createEntityStartOrchestrationSpanName("Counter"));
    }

    @Test
    void startEntityProcessingSpan_call_createsServerSpanUnderParent() {
        Span span = TracingHelper.startEntityProcessingSpan("Counter", "add", false, "@counter@c1", parentCtx());
        assertNotNull(span);
        TracingHelper.endEntityProcessingSpan(span, null);

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals("entity:Counter:add", sd.getName());
        assertEquals(SpanKind.SERVER, sd.getKind());
        assertEquals(TRACE_ID, sd.getTraceId());
        assertEquals(PARENT_SPAN_ID, sd.getParentSpanId());
        assertEquals("entity", attr(sd, "durabletask.type"));
        assertEquals("call_entity", attr(sd, "durabletask.task.operation"));
        assertEquals("@counter@c1", attr(sd, "durabletask.task.instance_id"));
        assertEquals(io.opentelemetry.api.trace.StatusCode.OK, sd.getStatus().getStatusCode());
    }

    @Test
    void startEntityProcessingSpan_signal_createsConsumerSpan() {
        Span span = TracingHelper.startEntityProcessingSpan("Counter", "add", true, "@counter@c1", parentCtx());
        assertNotNull(span);
        TracingHelper.endEntityProcessingSpan(span, null);

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals(SpanKind.CONSUMER, sd.getKind());
        assertEquals("signal_entity", attr(sd, "durabletask.task.operation"));
    }

    @Test
    void startEntityProcessingSpan_missingParent_returnsNullAndEmitsNothing() {
        assertNull(TracingHelper.startEntityProcessingSpan("Counter", "add", false, "@counter@c1", null));
        assertTrue(spanExporter.getFinishedSpanItems().isEmpty());
    }

    @Test
    void endEntityProcessingSpan_failure_setsErrorAndMessage() {
        Span span = TracingHelper.startEntityProcessingSpan("Counter", "add", false, "@counter@c1", parentCtx());
        TracingHelper.endEntityProcessingSpan(span, "boom");

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals(io.opentelemetry.api.trace.StatusCode.ERROR, sd.getStatus().getStatusCode());
        assertEquals("boom", attr(sd, "durabletask.entity.error_message"));
    }

    @Test
    void endEntityProcessingSpan_nullSpan_doesNotThrow() {
        assertDoesNotThrow(() -> TracingHelper.endEntityProcessingSpan(null, null));
    }

    @Test
    void entityProcessingSpan_omitsTaskNameVersionAndTaskId() {
        Span span = TracingHelper.startEntityProcessingSpan("Counter", "add", false, "@counter@c1", parentCtx());
        TracingHelper.endEntityProcessingSpan(span, null);

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertNull(attr(sd, "durabletask.task.name"));
        assertNull(attr(sd, "durabletask.task.version"));
        assertNull(attr(sd, "durabletask.task.task_id"));
    }

    @Test
    void emitEntityCallClientSpan_createsClientSpanWithSyntheticIdAndTimestamps() {
        java.time.Instant start = java.time.Instant.parse("2026-01-01T00:00:00Z");
        java.time.Instant end = java.time.Instant.parse("2026-01-01T00:00:05Z");
        String syntheticId = "abcdef1234567890";

        TracingHelper.emitEntityCallClientSpan(
                "Counter", "add", "@counter@c1", parentCtx(), start, end, syntheticId, null, null);

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals("entity:Counter:add", sd.getName());
        assertEquals(SpanKind.CLIENT, sd.getKind());
        assertEquals(syntheticId, sd.getSpanContext().getSpanId());
        assertEquals(PARENT_SPAN_ID, sd.getParentSpanId());
        assertEquals("call_entity", attr(sd, "durabletask.task.operation"));
        assertEquals("@counter@c1", attr(sd, "durabletask.event.target_instance_id"));
        assertEquals(io.opentelemetry.api.trace.StatusCode.UNSET, sd.getStatus().getStatusCode());
        assertEquals(start.getEpochSecond() * 1_000_000_000L + start.getNano(), sd.getStartEpochNanos());
        assertEquals(end.getEpochSecond() * 1_000_000_000L + end.getNano(), sd.getEndEpochNanos());
    }

    @Test
    void emitEntityCallClientSpan_withErrorDescription_setsError() {
        TracingHelper.emitEntityCallClientSpan(
                "Counter", "add", "@counter@c1", parentCtx(), null, null, null, null, "call timed out");

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals(io.opentelemetry.api.trace.StatusCode.ERROR, sd.getStatus().getStatusCode());
    }

    @Test
    void emitEntityCallClientSpan_missingParent_emitsNothing() {
        TracingHelper.emitEntityCallClientSpan(
                "Counter", "add", "@counter@c1", null, null, null, null, null, null);
        assertTrue(spanExporter.getFinishedSpanItems().isEmpty());
    }

    @Test
    void startEntitySignalProducerSpan_setsTargetAndSource() {
        Span span = TracingHelper.startEntitySignalProducerSpan(
                "Audit", "record", "@audit@a1", "@counter@c1", parentCtx(), null, null);
        assertNotNull(span);
        span.end();

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals("entity:Audit:record", sd.getName());
        assertEquals(SpanKind.PRODUCER, sd.getKind());
        assertEquals("signal_entity", attr(sd, "durabletask.task.operation"));
        assertEquals("@audit@a1", attr(sd, "durabletask.event.target_instance_id"));
        assertEquals("@counter@c1", attr(sd, "durabletask.task.instance_id"));
    }

    @Test
    void startEntitySignalProducerSpan_scheduledTime_setsAttribute() {
        Span span = TracingHelper.startEntitySignalProducerSpan(
                "Audit", "record", "@audit@a1", null, parentCtx(), null, "2026-01-01T00:00:00Z");
        span.end();

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals("2026-01-01T00:00:00Z", attr(sd, "durabletask.task.scheduled_time"));
        assertNull(attr(sd, "durabletask.task.instance_id"));
    }

    @Test
    void startEntityStartOrchestrationSpan_setsInvertedNameAndAttributes() {
        Span span = TracingHelper.startEntityStartOrchestrationSpan(
                "Counter", "@counter@c1", "orch-2", parentCtx(), null, null);
        assertNotNull(span);
        span.end();

        SpanData sd = spanExporter.getFinishedSpanItems().get(0);
        assertEquals("Counter:create_orchestration", sd.getName());
        assertEquals(SpanKind.PRODUCER, sd.getKind());
        assertEquals("entity", attr(sd, "durabletask.type"));
        assertEquals("orch-2", attr(sd, "durabletask.event.target_instance_id"));
        assertEquals("@counter@c1", attr(sd, "durabletask.task.instance_id"));
    }

    @Test
    void entityProducerSpans_missingParent_returnNull() {
        assertNull(TracingHelper.startEntitySignalProducerSpan(
                "Audit", "record", "@audit@a1", null, null, null, null));
        assertNull(TracingHelper.startEntityStartOrchestrationSpan(
                "Counter", "@counter@c1", "orch-2", null, null, null));
        assertTrue(spanExporter.getFinishedSpanItems().isEmpty());
    }

    // endregion
}
