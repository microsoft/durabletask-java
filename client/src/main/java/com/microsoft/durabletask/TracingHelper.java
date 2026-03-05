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
import java.lang.reflect.Field;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    private static final String TRACER_NAME = "Microsoft.DurableTask";
    private static final Logger logger = Logger.getLogger(TracingHelper.class.getName());

    /**
     * Cached reflection field for SdkSpan.context. Used to set span IDs
     * matching .NET SDK's DiagnosticActivityExtensions.SetSpanId() pattern.
     * Null if reflection is unavailable (no SDK on classpath, field renamed, etc.).
     */
    private static final Field SPAN_CONTEXT_FIELD;

    static {
        Field field = null;
        try {
            Class<?> sdkSpanClass = Class.forName("io.opentelemetry.sdk.trace.SdkSpan");
            field = sdkSpanClass.getDeclaredField("context");
            field.setAccessible(true);
        } catch (Throwable t) {
            logger.log(Level.FINE,
                    "SdkSpan.context field not accessible; span ID override disabled. " +
                    "Traces will use flat hierarchy instead of nested. " +
                    "This is expected when using the OTel API without the SDK.", t);
        }
        SPAN_CONTEXT_FIELD = field;
    }

    // Span type constants matching .NET SDK schema
    static final String TYPE_ORCHESTRATION = "orchestration";
    static final String TYPE_ACTIVITY = "activity";
    static final String TYPE_CREATE_ORCHESTRATION = "create_orchestration";
    static final String TYPE_TIMER = "timer";
    static final String TYPE_EVENT = "event";
    static final String TYPE_ORCHESTRATION_EVENT = "orchestration_event";

    // Attribute keys matching .NET SDK schema
    static final String ATTR_TYPE = "durabletask.type";
    static final String ATTR_TASK_NAME = "durabletask.task.name";
    static final String ATTR_INSTANCE_ID = "durabletask.task.instance_id";
    static final String ATTR_TASK_ID = "durabletask.task.task_id";
    static final String ATTR_FIRE_AT = "durabletask.fire_at";
    static final String ATTR_EVENT_TARGET_INSTANCE_ID = "durabletask.event.target_instance_id";

    private TracingHelper() {
        // Static utility class
    }

    /**
     * Sets the span ID of the given span using reflection on {@code SdkSpan.context}.
     * This mirrors .NET SDK's {@code DiagnosticActivityExtensions.SetSpanId()} which uses
     * reflection on {@code Activity._spanId} to enable trace deduplication and proper
     * parent-child hierarchy across orchestration dispatches.
     *
     * <p>If reflection is unavailable (no SDK on classpath, field renamed in future version),
     * this method silently does nothing and traces fall back to flat hierarchy.
     *
     * @param span   The span to modify, may be {@code null}.
     * @param spanId The 16-character hex span ID to set.
     */
    static void setSpanId(@Nullable Span span, @Nullable String spanId) {
        if (span == null || spanId == null || SPAN_CONTEXT_FIELD == null) {
            return;
        }
        try {
            SpanContext oldCtx = span.getSpanContext();
            if (!oldCtx.isValid()) {
                return;
            }
            SpanContext newCtx = SpanContext.create(
                    oldCtx.getTraceId(),
                    spanId,
                    oldCtx.getTraceFlags(),
                    oldCtx.getTraceState());
            SPAN_CONTEXT_FIELD.set(span, newCtx);
        } catch (Throwable t) {
            // Silently fall back to original span ID
            logger.log(Level.FINE, "Failed to set span ID via reflection", t);
        }
    }

    /**
     * Extracts the span ID from a W3C traceparent string.
     *
     * @param traceparent The traceparent string (e.g. "00-traceId-spanId-flags").
     * @return The span ID, or {@code null} if the traceparent is invalid.
     */
    @Nullable
    static String extractSpanIdFromTraceparent(@Nullable String traceparent) {
        if (traceparent == null || traceparent.isEmpty()) {
            return null;
        }
        String[] parts = traceparent.split("-");
        return parts.length >= 3 ? parts[2] : null;
    }

    /**
     * Captures the current OpenTelemetry span context as a protobuf {@code TraceContext}.
     *
     * @return A {@code TraceContext} proto, or {@code null} if there is no valid active span.
     */
    @Nullable
    static TraceContext getCurrentTraceContext() {
        return getCurrentTraceContext(Span.current());
    }

    /**
     * Captures a specific OpenTelemetry span's context as a protobuf {@code TraceContext}.
     *
     * @param span The span to capture context from, may be {@code null}.
     * @return A {@code TraceContext} proto, or {@code null} if the span has no valid context.
     */
    @Nullable
    static TraceContext getCurrentTraceContext(@Nullable Span span) {
        if (span == null || !span.getSpanContext().isValid()) {
            return null;
        }

        SpanContext spanContext = span.getSpanContext();

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
        return startSpanWithStartTime(name, traceContext, kind, attributes, null);
    }

    /**
     * Starts a new span as a child of the given trace context, optionally with a custom start time.
     * Used for orchestration spans where the engine provides the span start time.
     *
     * @param name        The span name.
     * @param traceContext The parent trace context from the protobuf message, may be {@code null}.
     * @param kind        The span kind, may be {@code null} (defaults to INTERNAL).
     * @param attributes  Optional span attributes, may be {@code null}.
     * @param startTime   Optional start time for the span, may be {@code null}.
     * @return The started {@code Span}. Caller must call {@link Span#end()} when done.
     */
    static Span startSpanWithStartTime(
            String name,
            @Nullable TraceContext traceContext,
            @Nullable SpanKind kind,
            @Nullable Map<String, String> attributes,
            @Nullable java.time.Instant startTime) {
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

        if (startTime != null) {
            spanBuilder.setStartTimestamp(startTime);
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

    /**
     * Creates a synthetic trace context with a new span ID under the parent trace.
     * Does NOT create or export a real span — just generates a new span ID for propagation.
     * The returned context is set as {@code parentTraceContext} on the action so the
     * server-side span becomes a child of the synthetic client span ID.
     * At completion time, {@link #emitRetroactiveClientSpan} creates a real span with this
     * span ID (via {@link #setSpanId}) to provide proper duration and attributes.
     *
     * @param parentContext  The parent trace context, may be {@code null}.
     * @return A {@code TraceContext} with a new span ID under the parent trace, or {@code null} if
     *         the parent context is invalid.
     */
    @Nullable
    static TraceContext createSyntheticClientContext(@Nullable TraceContext parentContext) {
        if (parentContext == null || parentContext.getTraceParent() == null
                || parentContext.getTraceParent().isEmpty()) {
            return null;
        }

        // Parse parent traceparent to get traceId and flags
        String[] parts = parentContext.getTraceParent().split("-");
        if (parts.length < 4) {
            return null;
        }

        String traceId = parts[1];
        String flags = parts[3];

        // Generate a new random span ID (16 hex chars)
        long randomId = java.util.concurrent.ThreadLocalRandom.current().nextLong();
        String newSpanId = String.format("%016x", randomId);

        String traceParent = String.format("00-%s-%s-%s", traceId, newSpanId, flags);

        TraceContext.Builder builder = TraceContext.newBuilder().setTraceParent(traceParent);
        if (parentContext.hasTraceState()) {
            builder.setTraceState(parentContext.getTraceState());
        }
        return builder.build();
    }

    /**
     * Emits a retroactive Client-kind span that covers the time from task scheduling to completion.
     * Uses reflection to set the span ID to match the original instant client span created at
     * scheduling time, so the server span becomes a child of this span in the trace hierarchy.
     * Matches .NET SDK pattern of paired Client+Server spans with SetSpanId().
     *
     * @param spanName           The span name (e.g. "activity:GetWeather").
     * @param parentContext      The parent trace context (orchestration context), may be {@code null}.
     * @param type               The durabletask.type value (e.g. "activity" or "orchestration").
     * @param taskName           The task name attribute value.
     * @param instanceId         The orchestration instance ID.
     * @param taskId             The task sequence ID.
     * @param startTime          The scheduling timestamp (span start time), may be {@code null}.
     * @param originalSpanId     The span ID from the original client span (for setSpanId), may be {@code null}.
     */
    static void emitRetroactiveClientSpan(
            String spanName,
            @Nullable TraceContext parentContext,
            String type,
            String taskName,
            @Nullable String instanceId,
            int taskId,
            @Nullable java.time.Instant startTime,
            @Nullable String originalSpanId) {
        Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(ATTR_TYPE, type)
                .setAttribute(ATTR_TASK_NAME, taskName)
                .setAttribute(ATTR_TASK_ID, String.valueOf(taskId));

        if (instanceId != null) {
            spanBuilder.setAttribute(ATTR_INSTANCE_ID, instanceId);
        }

        Context parentCtx = extractTraceContext(parentContext);
        if (parentCtx != null) {
            spanBuilder.setParent(parentCtx);
        }

        if (startTime != null) {
            spanBuilder.setStartTimestamp(startTime);
        }

        Span span = spanBuilder.startSpan();
        // Set the span ID to match the original client span so the server span is a child
        setSpanId(span, originalSpanId);
        span.end();
    }

    /**
     * Emits a timer span with duration from creation time to now.
     * Matches .NET SDK's {@code EmitTraceActivityForTimer} which spans from startTime to disposal time.
     *
     * @param orchestrationName The name of the orchestration that created the timer.
     * @param instanceId        The orchestration instance ID.
     * @param timerId           The timer event ID.
     * @param fireAt            The ISO-8601 formatted fire time.
     * @param parentContext     The parent trace context, may be {@code null}.
     * @param startTime         The timer creation time (span start), may be {@code null}.
     */
    static void emitTimerSpan(
            String orchestrationName,
            @Nullable String instanceId,
            int timerId,
            @Nullable String fireAt,
            @Nullable TraceContext parentContext,
            @Nullable java.time.Instant startTime) {
        Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        SpanBuilder spanBuilder = tracer.spanBuilder(
                TYPE_ORCHESTRATION + ":" + orchestrationName + ":" + TYPE_TIMER)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(ATTR_TYPE, TYPE_TIMER)
                .setAttribute(ATTR_TASK_NAME, orchestrationName)
                .setAttribute(ATTR_TASK_ID, String.valueOf(timerId));

        Context parentCtx = extractTraceContext(parentContext);
        if (parentCtx != null) {
            spanBuilder.setParent(parentCtx);
        }

        if (startTime != null) {
            spanBuilder.setStartTimestamp(startTime);
        }

        if (instanceId != null) {
            spanBuilder.setAttribute(ATTR_INSTANCE_ID, instanceId);
        }
        if (fireAt != null) {
            spanBuilder.setAttribute(ATTR_FIRE_AT, fireAt);
        }

        Span span = spanBuilder.startSpan();
        span.end();
    }

    /**
     * Emits a short-lived Producer span for an event raised from orchestrator (worker) or client.
     * Matches .NET SDK's {@code StartTraceActivityForEventRaisedFromWorker} and
     * {@code StartActivityForNewEventRaisedFromClient}.
     *
     * @param eventName         The name of the event being raised.
     * @param instanceId        The orchestration instance ID (worker-side), may be {@code null}.
     * @param targetInstanceId  The target orchestration instance ID, may be {@code null}.
     */
    static void emitEventSpan(
            String eventName,
            @Nullable String instanceId,
            @Nullable String targetInstanceId) {
        Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        SpanBuilder spanBuilder = tracer.spanBuilder(
                TYPE_ORCHESTRATION_EVENT + ":" + eventName)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(ATTR_TYPE, TYPE_EVENT)
                .setAttribute(ATTR_TASK_NAME, eventName);

        if (instanceId != null) {
            spanBuilder.setAttribute(ATTR_INSTANCE_ID, instanceId);
        }
        if (targetInstanceId != null) {
            spanBuilder.setAttribute(ATTR_EVENT_TARGET_INSTANCE_ID, targetInstanceId);
        }

        spanBuilder.startSpan().end();
    }
}
