/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.azure.functions.internal.spi.middleware.Middleware;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareChain;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareContext;
import com.microsoft.durabletask.OrchestrationRunner;
import com.microsoft.durabletask.interruption.ContinueAsNewInterruption;
import com.microsoft.durabletask.interruption.OrchestratorBlockedException;

/**
 * Durable Function Orchestration Middleware
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class OrchestrationMiddleware implements Middleware {

    private static final String ORCHESTRATION_TRIGGER = "DurableOrchestrationTrigger";

    @Override
    public void invoke(MiddlewareContext context, MiddlewareChain chain) throws Exception {
        String parameterName = context.getParameterName(ORCHESTRATION_TRIGGER);
        if (parameterName == null){
            chain.doNext(context);
            return;
        }
        String orchestratorRequestEncodedProtoBytes = (String) context.getParameterValue(parameterName);
        String orchestratorOutputEncodedProtoBytes = OrchestrationRunner.loadAndRun(orchestratorRequestEncodedProtoBytes, taskOrchestrationContext -> {
            try {
                context.updateParameterValue(parameterName, taskOrchestrationContext);
                chain.doNext(context);
                return context.getReturnValue();
            } catch (Exception e) {
                // The OrchestratorBlockedEvent will be wrapped into InvocationTargetException by using reflection to
                // invoke method. Thus get the cause to check if it's OrchestratorBlockedEvent.
                Throwable cause = e.getCause();
                if (cause instanceof OrchestratorBlockedException) {
                    throw (OrchestratorBlockedException) cause;
                }
                // The ContinueAsNewInterruption will be wrapped into InvocationTargetException by using reflection to
                // invoke method. Thus get the cause to check if it's ContinueAsNewInterruption.
                if (cause instanceof ContinueAsNewInterruption) {
                    throw (ContinueAsNewInterruption) cause;
                }
                throw new RuntimeException("Unexpected failure in the task execution", e);
            }
        });
        context.updateReturnValue(orchestratorOutputEncodedProtoBytes);
    }
}
