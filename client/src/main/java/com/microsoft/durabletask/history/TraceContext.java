// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;

/**
 * Distributed-tracing context (W3C Trace Context) associated with a history event.
 */
public final class TraceContext {
    private final String traceParent;
    private final String traceState;

    /**
     * Creates a new {@code TraceContext}.
     *
     * @param traceParent the W3C {@code traceparent} value
     * @param traceState  the W3C {@code tracestate} value, or {@code null}
     */
    public TraceContext(String traceParent, @Nullable String traceState) {
        this.traceParent = traceParent;
        this.traceState = traceState;
    }

    /** @return the W3C {@code traceparent} value. */
    public String getTraceParent() {
        return this.traceParent;
    }

    /** @return the W3C {@code tracestate} value, or {@code null} if not set. */
    @Nullable
    public String getTraceState() {
        return this.traceState;
    }
}
