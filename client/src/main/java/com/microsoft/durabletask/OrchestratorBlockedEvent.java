// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Control flow {@code Throwable} class for orchestrator functions. This {@code Throwable} must never be caught by user
 * code.
 * <p>
 * {@code OrchestratorBlockedEvent} is thrown when an orchestrator calls {@link Task#await} on an uncompleted task. The
 * purpose of throwing in this way is to halt execution of the orchestrator to save the current state and commit any
 * side effects. Catching {@code OrchestratorBlockedEvent} in user code could prevent the orchestration from saving
 * state and scheduling new tasks, resulting in the orchestration getting stuck.
 */
public final class OrchestratorBlockedEvent extends Throwable {
    OrchestratorBlockedEvent(String message) {
        super(message);
    }
}
