// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * History event recorded when an activity task is scheduled by an orchestration.
 */
public final class TaskScheduledEvent extends HistoryEvent {
    private final String name;
    private final String version;
    private final String input;
    private final TraceContext parentTraceContext;
    private final Map<String, String> tags;

    /**
     * Creates a new {@code TaskScheduledEvent}.
     *
     * @param eventId            the event sequence ID
     * @param timestamp          the event timestamp
     * @param name               the name of the scheduled activity
     * @param version            the activity version, or {@code null}
     * @param input              the serialized activity input, or {@code null}
     * @param parentTraceContext the parent distributed-tracing context, or {@code null}
     * @param tags               the activity tags, or {@code null}
     */
    public TaskScheduledEvent(
            int eventId,
            Instant timestamp,
            String name,
            @Nullable String version,
            @Nullable String input,
            @Nullable TraceContext parentTraceContext,
            @Nullable Map<String, String> tags) {
        super(eventId, timestamp);
        this.name = name;
        this.version = version;
        this.input = input;
        this.parentTraceContext = parentTraceContext;
        this.tags = tags != null ? Collections.unmodifiableMap(new HashMap<>(tags)) : Collections.emptyMap();
    }

    /** @return the name of the scheduled activity. */
    public String getName() {
        return this.name;
    }

    /** @return the activity version, or {@code null} if not set. */
    @Nullable
    public String getVersion() {
        return this.version;
    }

    /** @return the serialized activity input, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }

    /** @return the distributed-tracing context of the parent, or {@code null} if not set. */
    @Nullable
    public TraceContext getParentTraceContext() {
        return this.parentTraceContext;
    }

    /** @return the activity tags (never {@code null}; empty when none). */
    public Map<String, String> getTags() {
        return this.tags;
    }
}
