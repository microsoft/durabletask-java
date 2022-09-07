package com.microsoft.durabletask.azurefunctions.middleware;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.middleware.FunctionWorkerChain;
import com.microsoft.azure.functions.middleware.FunctionWorkerMiddleware;
import com.microsoft.durabletask.OrchestrationRunner;
import com.microsoft.durabletask.OrchestratorBlockedEvent;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.Map;

public class OrchestrationMiddleware implements FunctionWorkerMiddleware {
    private static final String ORCHESTRATION_TRIGGER = "DurableOrchestrationTrigger";
    private String inputName;
    @Override
    public void invoke(ExecutionContext context, FunctionWorkerChain next) throws Exception {

        if (!isOrchestrationTrigger(context)) {
            next.doNext(context);
            return;
        }

        Map<String, String> argumentPayloadMap = context.getArgumentPayloadMap();
        String base64EncodedOrchestratorRequest = argumentPayloadMap.get(this.inputName);

        String orchestratorOutput  = OrchestrationRunner.loadAndRun(base64EncodedOrchestratorRequest, ctx -> {
            try {
                context.setInputArgument(this.inputName, ctx);
                next.doNext(context);
                return context.getReturnValue();
            } catch (Exception e) {
                if (e.getCause() instanceof OrchestratorBlockedEvent) {
                    throw (OrchestratorBlockedEvent) e.getCause();
                }
                throw new RuntimeException("Unexpected failure in the task execution", e);
            }
        });

        context.setImplicitOutput(orchestratorOutput);
    }

    private boolean isOrchestrationTrigger(ExecutionContext context) {
        Map<String, Parameter> paramInfoMap = context.getParamInfoMap();
        for (Map.Entry<String, Parameter> entry : paramInfoMap.entrySet()){
            Parameter parameter = entry.getValue();
            if (paramHasAnnotation(parameter)){
                this.inputName = entry.getKey();
                return true;
            }
        }
        return false;
    }

    private boolean paramHasAnnotation(Parameter parameter) {
        for (Annotation annotation : parameter.getAnnotations()) {
            if (annotation.annotationType().getSimpleName().equals(ORCHESTRATION_TRIGGER)) {
                return true;
            }
        }
        return false;
    }
}
