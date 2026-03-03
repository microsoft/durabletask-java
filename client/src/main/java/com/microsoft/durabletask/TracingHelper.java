// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TraceContext;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for OpenTelemetry distributed tracing integration.
 * <p>
 * Provides helpers for propagating W3C Trace Context between orchestrations,
 * activities, and sub-orchestrations via the {@code TraceContext} protobuf message.
 * <p>
 * When no OpenTelemetry SDK is configured, the API returns no-op implementations,
 * so all methods in this class are safe to call without any tracing overhead.
 */
final class TracingHelper {

    private static final String TRACER_NAME = "durabletask";

    private TracingHelper() {
        // Static utility class
    }

    /**
     * Captures the current OpenTelemetry span context as a protobuf {@code TraceContext}.
     *
     * @return A {@code TraceContext} proto, or {@code null} if there is no valid active span.
     */
    @Nullable
    static TraceContext getCurrentTraceContext() {
        Span currentSpan = Span.current();
        if (currentSpan == null || !currentSpan.getSpanContext().isValid()) {
            return null;
        }

        SpanContext spanContext = currentSpan.getSpanContext();

        // Construct the traceparent according to the W3C Trace Context specification
        // https://www.w3.org/TR/trace-context/#traceparent-header
        String traceParent = String.format("00-%s-%s-%02x",
                spanContext.getTraceId(),
                spanContext.getSpanId(),
                spanContext.getTraceFlags().asByte());

        String traceState = spanContext.getTraceState().asMap()
                .entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(","));

        TraceContext.Builder builder = TraceContext.newBuilder()
                .setTraceParent(traceParent);

        if (traceState != null && !traceState.isEmpty()) {
            builder.setTraceState(StringValue.of(traceState));
        }

        return builder.build();
    }

    /**
     * Converts a protobuf {@code TraceContext} into an OpenTelemetry {@code Context}
     * that can be used as a parent for new spans.
     *
     * @param protoCtx The protobuf trace context, may be {@code null}.
     * @return An OpenTelemetry {@code Context}, or {@code null} if the input is empty/null.
     */
    @Nullable
    static Context extractTraceContext(@Nullable TraceContext protoCtx) {
        if (protoCtx == null) {
            return null;
        }

        String traceParent = protoCtx.getTraceParent();
        if (traceParent == null || traceParent.isEmpty()) {
            return null;
        }

        // Parse W3C traceparent: 00-<traceId>-<spanId>-<flags>
        String[] parts = traceParent.split("-");
        if (parts.length < 4) {
            return null;
        }

        String traceId = parts[1];
        String spanId = parts[2];
        byte flags = 0;
        try {
            flags = (byte) Integer.parseInt(parts[3], 16);
        } catch (NumberFormatException e) {
            // Use default flags
        }

        // Parse tracestate if present
        TraceState traceState = TraceState.getDefault();
        if (protoCtx.hasTraceState() && protoCtx.getTraceState().getValue() != null
                && !protoCtx.getTraceState().getValue().isEmpty()) {
            TraceStateBuilder tsBuilder = TraceState.builder();
            String[] entries = protoCtx.getTraceState().getValue().split(",");
            for (String entry : entries) {
                String[] kv = entry.split("=", 2);
                if (kv.length == 2) {
                    tsBuilder.put(kv[0].trim(), kv[1].trim());
                }
            }
            traceState = tsBuilder.build();
        }

        SpanContext remoteContext = SpanContext.createFromRemoteParent(
                traceId, spanId, TraceFlags.fromByte(flags), traceState);

        if (!remoteContext.isValid()) {
            return null;
        }

        return Context.current().with(Span.wrap(remoteContext));
    }

    /**
     * Starts a new span as a child of the given trace context.
     *
     * @param name        The span name (e.g. "activity:say_hello").
     * @param traceContext The parent trace context from the protobuf message, may be {@code null}.
     * @param kind        The span kind, may be {@code null} (defaults to INTERNAL).
     * @param attributes  Optional span attributes, may be {@code null}.
     * @return The started {@code Span}. Caller must call {@link Span#end()} when done.
     */
    static Span startSpan(
            String name,
            @Nullable TraceContext traceContext,
            @Nullable SpanKind kind,
            @Nullable Map<String, String> attributes) {
        Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        SpanBuilder spanBuilder = tracer.spanBuilder(name);

        if (kind != null) {
            spanBuilder.setSpanKind(kind);
        }

        Context parentCtx = extractTraceContext(traceContext);
        if (parentCtx != null) {
            spanBuilder.setParent(parentCtx);
        }

        if (attributes != null) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                spanBuilder.setAttribute(entry.getKey(), entry.getValue());
            }
        }

        Span span = spanBuilder.startSpan();
        return span;
    }

    /**
     * Ends the given span, optionally recording an error.
     *
     * @param span  The span to end, may be {@code null}.
     * @param error If non-null, the exception is recorded on the span.
     */
    static void endSpan(@Nullable Span span, @Nullable Throwable error) {
        if (span == null) {
            return;
        }
        if (error != null) {
            span.setStatus(StatusCode.ERROR, error.getMessage());
            span.recordException(error);
        }
        span.end();
    }
}
