package com.functions;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import java.net.URI;
import java.util.*;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

/**
 * End-to-end test for the DurableHttp.callHttp API.
 */
public class CallHttpFunction {

    /**
     * HTTP trigger that starts an orchestration which makes a durable HTTP GET call.
     */
    @FunctionName("StartCallHttp")
    public HttpResponseMessage startCallHttp(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Starting callHttp orchestration.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("CallHttpOrchestrator");
        context.getLogger().info("Created callHttp orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * Orchestrator that makes a durable HTTP GET request and returns the status code.
     */
    @FunctionName("CallHttpOrchestrator")
    public int callHttpOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://httpbin.org/get"));
        DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
        return response.getStatusCode();
    }
}
