// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Functional interface for inline orchestrator functions.
 */
@FunctionalInterface
public interface OrchestratorFunction<R> {
    R apply(TaskOrchestrationContext ctx) throws OrchestratorBlockedEvent, TaskFailedException;
}