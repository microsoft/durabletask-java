package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.azure.functions.internal.MiddlewareContext;
import com.microsoft.azure.functions.middleware.FunctionWorkerChain;
import com.microsoft.azure.functions.middleware.FunctionWorkerMiddleware;
import com.microsoft.durabletask.OrchestrationRunner;
import com.microsoft.durabletask.OrchestratorBlockedEvent;
import com.microsoft.durabletask.TaskFailedException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.Map;

/**
 * Durable Function Orchestration Middleware
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class OrchestrationMiddleware implements FunctionWorkerMiddleware {

    private static final String ORCHESTRATION_TRIGGER = "DurableOrchestrationTrigger";

    @Override
    public void invoke(MiddlewareContext context, FunctionWorkerChain next) throws Exception {
        Pair pair = isOrchestrationTrigger(context);
        if (!pair.isOrchestrationTrigger){
            next.doNext(context);
            return;
        }
        String orchestratorRequestProtoBytes = (String) context.getParameterPayloadByName(pair.parameterName);
        String orchestratorOutput  = OrchestrationRunner.loadAndRun(orchestratorRequestProtoBytes, ctx -> {
            try {
                context.updateParameterPayloadByName(pair.parameterName, ctx);
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

    private static Pair isOrchestrationTrigger(MiddlewareContext context) {
        for (Map.Entry<String, Parameter> entry : context.getParameterMap().entrySet()){
            if (isOrchestrationTrigger(entry.getValue())){
                return new Pair(true, entry.getKey());
            }
        }
        return new Pair(false, null);
    }

    private static boolean isOrchestrationTrigger(Parameter parameter){
        Annotation[] annotations = parameter.getAnnotations();
        for (Annotation annotation : annotations) {
            if(annotation.annotationType().getSimpleName().equals(ORCHESTRATION_TRIGGER)){
                return true;
            }
        }
        return false;
    }

    private static class Pair{
        private final boolean isOrchestrationTrigger;
        private final String parameterName;

        private Pair(boolean isOrchestrationTrigger, String parameterName) {
            this.isOrchestrationTrigger = isOrchestrationTrigger;
            this.parameterName = parameterName;
        }
    }
}
