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
        attrs.put("durabletask.task.name", "test-activity");
        attrs.put("durabletask.task.instance_id", "abc123");

        Span span = TracingHelper.startSpan("activity:test-activity", null, SpanKind.INTERNAL, attrs);
        assertNotNull(span);
        span.end();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        SpanData spanData = spans.get(0);
        assertEquals("activity:test-activity", spanData.getName());
        assertEquals("test-activity", spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.name")));
        assertEquals("abc123", spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("durabletask.task.instance_id")));
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
}
