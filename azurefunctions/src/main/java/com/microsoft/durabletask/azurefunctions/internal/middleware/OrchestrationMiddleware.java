package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.azure.functions.internal.MiddlewareContext;
import com.microsoft.azure.functions.middleware.FunctionMiddlewareChain;
import com.microsoft.azure.functions.middleware.FunctionWorkerMiddleware;
import com.microsoft.durabletask.OrchestrationRunner;
import com.microsoft.durabletask.OrchestratorBlockedEvent;
import java.util.Optional;

/**
 * Durable Function Orchestration Middleware
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class OrchestrationMiddleware implements FunctionWorkerMiddleware {

    private static final String ORCHESTRATION_TRIGGER = "DurableOrchestrationTrigger";

    @Override
    public void invoke(MiddlewareContext context, FunctionMiddlewareChain next) throws Exception {
        Optional<String> parameterName = context.getParameterName(ORCHESTRATION_TRIGGER);
        if (!parameterName.isPresent()){
            next.doNext(context);
            return;
        }
        String orchestratorRequestProtoBytes = (String) context.getParameterPayloadByName(parameterName.get());
        String orchestratorOutput  = OrchestrationRunner.loadAndRun(orchestratorRequestProtoBytes, ctx -> {
            try {
                context.updateParameterPayloadByName(parameterName.get(), ctx);
                next.doNext(context);
                return context.getReturnValue();
            } catch (Exception e) {
                Throwable cause = e.getCause();
                if (cause instanceof OrchestratorBlockedEvent){
                    throw (OrchestratorBlockedEvent) cause;
                }
                throw new RuntimeException("Unexpected failure in the task execution", e);
            }
        });
        context.setMiddlewareOutput(orchestratorOutput);
    }
}
