package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.util.Optional;

/**
 * Sample demonstrating distributed tracing with Durable Functions.
 * <p>
 * Trace context is automatically propagated from the HTTP trigger through the
 * orchestration to each activity and sub-orchestration. When Application Insights
 * or an OpenTelemetry exporter is configured, you will see correlated traces
 * across the entire workflow.
 */
public class TracingChain {

    @FunctionName("StartTracingChain")
    public HttpResponseMessage startTracingChain(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Starting TracingChain orchestration");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("TracingChain");
        context.getLogger().info("Created orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * Orchestration that chains activities and a sub-orchestration.
     * Trace context flows from the client through each step.
     */
    @FunctionName("TracingChain")
    public String tracingChain(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String input = ctx.getInput(String.class);
        if (input == null) {
            input = "Hello";
        }

        // Each activity execution creates a child span under the orchestration span
        String step1 = ctx.callActivity("TracingReverse", input, String.class).await();
        String step2 = ctx.callActivity("TracingCapitalize", step1, String.class).await();

        // Sub-orchestration also propagates trace context
        String result = ctx.callSubOrchestrator("TracingChildOrch", step2, String.class).await();

        return result;
    }

    /**
     * Sub-orchestration that receives propagated trace context.
     */
    @FunctionName("TracingChildOrch")
    public String tracingChildOrch(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String input = ctx.getInput(String.class);
        return ctx.callActivity("TracingAddSuffix", input, String.class).await();
    }

    @FunctionName("TracingReverse")
    public String tracingReverse(
            @DurableActivityTrigger(name = "input") String input,
            final ExecutionContext context) {
        context.getLogger().info("TracingReverse: " + input);
        return new StringBuilder(input).reverse().toString();
    }

    @FunctionName("TracingCapitalize")
    public String tracingCapitalize(
            @DurableActivityTrigger(name = "input") String input,
            final ExecutionContext context) {
        context.getLogger().info("TracingCapitalize: " + input);
        return input.toUpperCase();
    }

    @FunctionName("TracingAddSuffix")
    public String tracingAddSuffix(
            @DurableActivityTrigger(name = "input") String input,
            final ExecutionContext context) {
        context.getLogger().info("TracingAddSuffix: " + input);
        return input + "-traced";
    }
}
