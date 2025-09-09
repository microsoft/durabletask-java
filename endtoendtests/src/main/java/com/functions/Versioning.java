package com.functions;

import java.util.Optional;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import com.microsoft.durabletask.NewSubOrchestrationInstanceOptions;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

public class Versioning {
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("StartVersionedOrchestration")
    public HttpResponseMessage startOrchestration(
        @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
        @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
        final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String queryVersion = request.getQueryParameters().getOrDefault("version", "");
        context.getLogger().info(String.format("Received version '%s' from the query string", queryVersion));
        String instanceId = null;
        if (queryVersion.equals("default")) {
            instanceId = client.scheduleNewOrchestrationInstance("VersionedOrchestrator");
        } else {
            instanceId = client.scheduleNewOrchestrationInstance("VersionedOrchestrator", new NewOrchestrationInstanceOptions().setVersion(queryVersion));
        }
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("StartVersionedSubOrchestration")
    public HttpResponseMessage startSubOrchestration(
        @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
        @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
        final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String queryVersion = request.getQueryParameters().getOrDefault("version", "");
        String instanceId = null;
        if (queryVersion.equals("default")) {
            instanceId = client.scheduleNewOrchestrationInstance("VersionedSubOrchestrator");
        } else {
            instanceId = client.scheduleNewOrchestrationInstance("VersionedSubOrchestrator", queryVersion);
        }
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("VersionedOrchestrator")
    public String versionedOrchestrator(
        @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        return ctx.callActivity("SayVersion", ctx.getVersion(), String.class).await();
    }

    @FunctionName("VersionedSubOrchestrator")
    public String versionedSubOrchestrator(
        @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String subVersion = ctx.getInput(String.class);
        NewSubOrchestrationInstanceOptions options = new NewSubOrchestrationInstanceOptions();
        if (subVersion != null) {
            options.setVersion(subVersion);
        }
        return ctx.callSubOrchestrator("SubOrchestrator", null, null, options, String.class).await();
    }

    @FunctionName("SubOrchestrator")
    public String subOrchestrator(
        @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        return ctx.callActivity("SayVersion", ctx.getVersion(), String.class).await();
    }

    @FunctionName("SayVersion")
    public String sayVersion(
        @DurableActivityTrigger(name = "version") String version,
        final ExecutionContext context) {
        version = version.replaceAll("\"", "");
        context.getLogger().info(String.format("Called with version: '%s'", version));
        return String.format("Version: '%s'", version);
    }
}
