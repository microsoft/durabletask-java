// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * History event recorded when an orchestration instance begins execution.
 */
public final class ExecutionStartedEvent extends HistoryEvent {
    private final String name;
    private final String version;
    private final String input;
    private final OrchestrationInstance orchestrationInstance;
    private final ParentInstanceInfo parentInstance;
    private final Instant scheduledStartTimestamp;
    private final TraceContext parentTraceContext;
    private final String orchestrationSpanId;
    private final Map<String, String> tags;

    /**
     * Creates a new {@code ExecutionStartedEvent}.
     *
     * @param eventId                 the event sequence ID
     * @param timestamp               the event timestamp
     * @param name                    the orchestrator name
     * @param version                 the orchestrator version, or {@code null}
     * @param input                   the serialized orchestration input, or {@code null}
     * @param orchestrationInstance   the orchestration instance, or {@code null}
     * @param parentInstance          the parent orchestration info, or {@code null}
     * @param scheduledStartTimestamp the scheduled start time for delayed starts, or {@code null}
     * @param parentTraceContext      the parent distributed-tracing context, or {@code null}
     * @param orchestrationSpanId     the orchestration's tracing span ID, or {@code null}
     * @param tags                    the orchestration tags, or {@code null}
     */
    public ExecutionStartedEvent(
            int eventId,
            Instant timestamp,
            String name,
            @Nullable String version,
            @Nullable String input,
            @Nullable OrchestrationInstance orchestrationInstance,
            @Nullable ParentInstanceInfo parentInstance,
            @Nullable Instant scheduledStartTimestamp,
            @Nullable TraceContext parentTraceContext,
            @Nullable String orchestrationSpanId,
            @Nullable Map<String, String> tags) {
        super(eventId, timestamp);
        this.name = name;
        this.version = version;
        this.input = input;
        this.orchestrationInstance = orchestrationInstance;
        this.parentInstance = parentInstance;
        this.scheduledStartTimestamp = scheduledStartTimestamp;
        this.parentTraceContext = parentTraceContext;
        this.orchestrationSpanId = orchestrationSpanId;
        this.tags = tags != null ? Collections.unmodifiableMap(new HashMap<>(tags)) : Collections.emptyMap();
    }

    /** @return the name of the orchestrator. */
    public String getName() {
        return this.name;
    }

    /** @return the orchestrator version, or {@code null} if not set. */
    @Nullable
    public String getVersion() {
        return this.version;
    }

    /** @return the serialized orchestration input, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }

    /** @return the orchestration instance, or {@code null} if not set. */
    @Nullable
    public OrchestrationInstance getOrchestrationInstance() {
        return this.orchestrationInstance;
    }

    /** @return the parent orchestration info if this is a sub-orchestration, otherwise {@code null}. */
    @Nullable
    public ParentInstanceInfo getParentInstance() {
        return this.parentInstance;
    }

    /** @return the scheduled start time for delayed starts, or {@code null} if started immediately. */
    @Nullable
    public Instant getScheduledStartTimestamp() {
        return this.scheduledStartTimestamp;
    }

    /** @return the distributed-tracing context of the parent, or {@code null} if not set. */
    @Nullable
    public TraceContext getParentTraceContext() {
        return this.parentTraceContext;
    }

    /** @return the orchestration's tracing span ID, or {@code null} if not set. */
    @Nullable
    public String getOrchestrationSpanId() {
        return this.orchestrationSpanId;
    }

    /** @return the orchestration tags (never {@code null}; empty when none). */
    public Map<String, String> getTags() {
        return this.tags;
    }
}
