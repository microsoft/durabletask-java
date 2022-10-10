// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Functional interface for inline orchestrator functions.
 * <p>
 * See the description of {@link TaskOrchestration} for more information about how to correctly implement orchestrators.
 */
@FunctionalInterface
public interface OrchestratorFunction<R> {
    /**
     * Executes an orchestrator function and returns a result to use as the orchestration output.
     * <p>
     * This functional interface is designed to support implementing orchestrators as lambda functions. It's intended to
     * be very similar to {@link java.util.function.Function}, but with a signature that's specific to orchestrators.
     *
     * @param ctx the orchestration context, which provides access to additional context for the current orchestration
     *            execution
     * @return the serializable output of the orchestrator function
     */
    R apply(TaskOrchestrationContext ctx);
}