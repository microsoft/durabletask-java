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

    private static final String TRACER_NAME = "Microsoft.DurableTask";

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
     * Creates a short-lived Client-kind span for scheduling an activity or sub-orchestration,
     * captures its trace context as a protobuf {@code TraceContext}, and ends the span immediately.
     * This mirrors the .NET SDK pattern of paired Client+Server spans.
     *
     * @param spanName       The span name (e.g. "activity:GetWeather").
     * @param parentContext  The parent trace context from the orchestration, may be {@code null}.
     * @param type           The durabletask.type value (e.g. "activity").
     * @param taskName       The task name attribute value.
     * @param instanceId     The orchestration instance ID.
     * @param taskId         The task sequence ID.
     * @return A {@code TraceContext} captured from the Client span, or the original parentContext if tracing is unavailable.
     */
    @Nullable
    static TraceContext createClientSpan(
            String spanName,
            @Nullable TraceContext parentContext,
            String type,
            String taskName,
            @Nullable String instanceId,
            int taskId) {

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

        Span clientSpan = spanBuilder.startSpan();

        // Capture the client span's context before ending it
        TraceContext captured;
        try {
            SpanContext sc = clientSpan.getSpanContext();
            if (sc.isValid()) {
                String traceParent = String.format("00-%s-%s-%02x",
                        sc.getTraceId(), sc.getSpanId(), sc.getTraceFlags().asByte());

                String traceState = sc.getTraceState().asMap()
                        .entrySet()
                        .stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining(","));

                TraceContext.Builder builder = TraceContext.newBuilder().setTraceParent(traceParent);
                if (traceState != null && !traceState.isEmpty()) {
                    builder.setTraceState(StringValue.of(traceState));
                }
                captured = builder.build();
            } else {
                captured = parentContext;
            }
        } finally {
            clientSpan.end();
        }
        return captured;
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
     * Emits a short-lived span for a durable timer that has fired.
     * Matches .NET SDK's {@code EmitTraceActivityForTimer}.
     *
     * @param orchestrationName The name of the orchestration that created the timer.
     * @param instanceId        The orchestration instance ID.
     * @param timerId           The timer event ID.
     * @param fireAt            The ISO-8601 formatted fire time.
     * @param parentContext     The parent trace context, may be {@code null}.
     */
    static void emitTimerSpan(
            String orchestrationName,
            @Nullable String instanceId,
            int timerId,
            @Nullable String fireAt,
            @Nullable TraceContext parentContext) {
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
     * Emits a short-lived Producer span for an event raised from the orchestrator (worker side).
     * Matches .NET SDK's {@code StartTraceActivityForEventRaisedFromWorker}.
     *
     * @param eventName         The name of the event being raised.
     * @param instanceId        The orchestration instance ID sending the event.
     * @param targetInstanceId  The target orchestration instance ID, may be {@code null}.
     */
    static void emitEventRaisedFromWorkerSpan(
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

        Span span = spanBuilder.startSpan();
        span.end();
    }

    /**
     * Emits a short-lived Producer span for an event raised from the client.
     * Matches .NET SDK's {@code StartActivityForNewEventRaisedFromClient}.
     *
     * @param eventName         The name of the event being raised.
     * @param targetInstanceId  The target orchestration instance ID.
     */
    static void emitEventRaisedFromClientSpan(
            String eventName,
            @Nullable String targetInstanceId) {
        Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        SpanBuilder spanBuilder = tracer.spanBuilder(
                TYPE_ORCHESTRATION_EVENT + ":" + eventName)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(ATTR_TYPE, TYPE_EVENT)
                .setAttribute(ATTR_TASK_NAME, eventName);

        if (targetInstanceId != null) {
            spanBuilder.setAttribute(ATTR_EVENT_TARGET_INSTANCE_ID, targetInstanceId);
        }

        Span span = spanBuilder.startSpan();
        span.end();
    }
}
