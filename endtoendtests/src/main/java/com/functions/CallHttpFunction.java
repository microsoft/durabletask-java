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
     * Simple HTTP-triggered endpoint used as a local target for durable HTTP call tests.
     * Returns a 200 OK response, avoiding external service dependencies.
     */
    @FunctionName("EchoHttp")
    public HttpResponseMessage echoHttp(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("EchoHttp invoked.");
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("{\"status\":\"ok\"}")
                .build();
    }

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
     * Orchestrator that makes a durable HTTP GET request to the local EchoHttp endpoint
     * and returns the status code.
     * <p>
     * The e2e tests run in Docker where the Azure Functions host listens on port 80 internally.
     * The callHttp call is executed by the Durable Task extension in the same container,
     * so localhost:80 reaches the host directly.
     */
    @FunctionName("CallHttpOrchestrator")
    public int callHttpOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("http://localhost:80/api/EchoHttp"));
        DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
        return response.getStatusCode();
    }
}
