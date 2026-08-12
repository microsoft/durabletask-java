// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an orchestration instance reaches a terminal state.
 */
public final class ExecutionCompletedEvent extends HistoryEvent {
    private final OrchestrationRuntimeStatus orchestrationStatus;
    private final String result;
    private final FailureDetails failureDetails;

    /**
     * Creates a new {@code ExecutionCompletedEvent}.
     *
     * @param eventId             the event sequence ID
     * @param timestamp           the event timestamp
     * @param orchestrationStatus the terminal runtime status
     * @param result              the serialized orchestration output, or {@code null}
     * @param failureDetails      the failure details if the orchestration failed, or {@code null}
     */
    public ExecutionCompletedEvent(
            int eventId,
            Instant timestamp,
            OrchestrationRuntimeStatus orchestrationStatus,
            @Nullable String result,
            @Nullable FailureDetails failureDetails) {
        super(eventId, timestamp);
        this.orchestrationStatus = orchestrationStatus;
        this.result = result;
        this.failureDetails = failureDetails;
    }

    /** @return the terminal runtime status of the orchestration. */
    public OrchestrationRuntimeStatus getOrchestrationStatus() {
        return this.orchestrationStatus;
    }

    /** @return the serialized orchestration output, or {@code null} if none. */
    @Nullable
    public String getResult() {
        return this.result;
    }

    /** @return the failure details if the orchestration failed, otherwise {@code null}. */
    @Nullable
    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }
}
