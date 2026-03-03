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
    void createClientSpan_createsClientKindSpan_withNewSpanId() {
        // Create a parent context
        Tracer tracer = openTelemetry.getTracer("test");
        Span parentSpan = tracer.spanBuilder("parent-orch").startSpan();
        TraceContext parentCtx;
        try (Scope ignored = parentSpan.makeCurrent()) {
            parentCtx = TracingHelper.getCurrentTraceContext();
        } finally {
            parentSpan.end();
        }
        assertNotNull(parentCtx);
        spanExporter.reset();

        // Create a client span
        TraceContext clientCtx = TracingHelper.createClientSpan(
                "activity:GetWeather", parentCtx,
                TracingHelper.TYPE_ACTIVITY, "GetWeather", "instance-123", 3);

        assertNotNull(clientCtx);
        assertNotNull(clientCtx.getTraceParent());

        // Client span should have the same trace ID but different span ID
        String parentTraceId = parentCtx.getTraceParent().split("-")[1];
        String clientTraceId = clientCtx.getTraceParent().split("-")[1];
        String parentSpanId = parentCtx.getTraceParent().split("-")[2];
        String clientSpanId = clientCtx.getTraceParent().split("-")[2];
        assertEquals(parentTraceId, clientTraceId, "Should share the same trace ID");
        assertNotEquals(parentSpanId, clientSpanId, "Should have a new span ID");

        // Verify the exported client span has correct attributes and kind
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("activity:GetWeather", sd.getName());
        assertEquals(SpanKind.CLIENT, sd.getKind());
        assertEquals("activity", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
        assertEquals("GetWeather", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.name")));
        assertEquals("instance-123", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.instance_id")));
        assertEquals("3", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.task_id")));
    }

    @Test
    void createClientSpan_withNullParent_returnsNull() {
        TraceContext result = TracingHelper.createClientSpan(
                "activity:Test", null, TracingHelper.TYPE_ACTIVITY, "Test", null, 0);
        // When parent is null, the span has no parent but still creates a valid context
        // (or returns null if the tracer returns an invalid span)
        // With a registered SDK, it should create a root span
        assertNotNull(result);
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
                "GetWeather", "instance-1", 3, startTime);

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
    void emitEventRaisedFromWorkerSpan_createsProducerSpan() {
        TracingHelper.emitEventRaisedFromWorkerSpan("ApprovalEvent", "orch-1", "target-orch-2");

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
    void emitEventRaisedFromClientSpan_createsProducerSpan() {
        TracingHelper.emitEventRaisedFromClientSpan("ApprovalEvent", "target-orch-1");

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData sd = spans.get(0);
        assertEquals("orchestration_event:ApprovalEvent", sd.getName());
        assertEquals(SpanKind.PRODUCER, sd.getKind());
        assertEquals("event", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.type")));
        assertEquals("target-orch-1", sd.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.event.target_instance_id")));
    }
}
