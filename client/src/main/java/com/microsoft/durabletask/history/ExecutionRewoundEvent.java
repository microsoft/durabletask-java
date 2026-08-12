// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * History event recorded when an orchestration instance is rewound to a previous good state.
 */
public final class ExecutionRewoundEvent extends HistoryEvent {
    private final String reason;
    private final String parentExecutionId;
    private final String instanceId;
    private final TraceContext parentTraceContext;
    private final String name;
    private final String version;
    private final String input;
    private final ParentInstanceInfo parentInstance;
    private final Map<String, String> tags;

    /**
     * Creates a new {@code ExecutionRewoundEvent}.
     *
     * @param eventId            the event sequence ID
     * @param timestamp          the event timestamp
     * @param reason             the reason for the rewind, or {@code null}
     * @param parentExecutionId  the parent execution ID (sub-orchestration rewind only), or {@code null}
     * @param instanceId         the instance ID (sub-orchestration rewind only), or {@code null}
     * @param parentTraceContext the parent distributed-tracing context, or {@code null}
     * @param name               the orchestrator name, or {@code null}
     * @param version            the orchestrator version, or {@code null}
     * @param input              the serialized input, or {@code null}
     * @param parentInstance     the parent orchestration info, or {@code null}
     * @param tags               the orchestration tags, or {@code null}
     */
    public ExecutionRewoundEvent(
            int eventId,
            Instant timestamp,
            @Nullable String reason,
            @Nullable String parentExecutionId,
            @Nullable String instanceId,
            @Nullable TraceContext parentTraceContext,
            @Nullable String name,
            @Nullable String version,
            @Nullable String input,
            @Nullable ParentInstanceInfo parentInstance,
            @Nullable Map<String, String> tags) {
        super(eventId, timestamp);
        this.reason = reason;
        this.parentExecutionId = parentExecutionId;
        this.instanceId = instanceId;
        this.parentTraceContext = parentTraceContext;
        this.name = name;
        this.version = version;
        this.input = input;
        this.parentInstance = parentInstance;
        this.tags = tags != null ? Collections.unmodifiableMap(new HashMap<>(tags)) : Collections.emptyMap();
    }

    /** @return the reason for the rewind, or {@code null} if none. */
    @Nullable
    public String getReason() {
        return this.reason;
    }

    /** @return the parent execution ID (sub-orchestration rewind only), or {@code null}. */
    @Nullable
    public String getParentExecutionId() {
        return this.parentExecutionId;
    }

    /** @return the instance ID (sub-orchestration rewind only), or {@code null}. */
    @Nullable
    public String getInstanceId() {
        return this.instanceId;
    }

    /** @return the parent distributed-tracing context, or {@code null} if not set. */
    @Nullable
    public TraceContext getParentTraceContext() {
        return this.parentTraceContext;
    }

    /** @return the orchestrator name, or {@code null} if not set. */
    @Nullable
    public String getName() {
        return this.name;
    }

    /** @return the orchestrator version, or {@code null} if not set. */
    @Nullable
    public String getVersion() {
        return this.version;
    }

    /** @return the serialized input, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }

    /** @return the parent orchestration info, or {@code null} if not set. */
    @Nullable
    public ParentInstanceInfo getParentInstance() {
        return this.parentInstance;
    }

    /** @return the orchestration tags (never {@code null}; empty when none). */
    public Map<String, String> getTags() {
        return this.tags;
    }
}
