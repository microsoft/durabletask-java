package com.functions;

import com.microsoft.azure.functions.*;
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
 * Azure Durable Functions with HTTP trigger - Rewind instance sample.
 */
public class RewindInstance {
    private static int approvalFlag = 0;

    /**
     * This HTTP-triggered function starts the approval orchestration.
     */
    @FunctionName("ApprovalWorkflowOrchestration")
    public HttpResponseMessage approvalWorkflowOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) throws InterruptedException {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("ApprovalWorkflow");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("ApprovalWorkflow")
    public int approvalWorkflow(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        int result = 0;
        result += ctx.callActivity("RequestPrimaryApproval", 1, Integer.class).await();
        result += ctx.callActivity("RequestSecondaryApproval", 1, Integer.class).await();
        return result;
    }

    /**
     * This is the activity function that gets invoked by the approval orchestration.
     */
    @FunctionName("RequestPrimaryApproval")
    public int requestPrimaryApproval(
            @DurableActivityTrigger(name = "name") int number,
            final ExecutionContext context) {
        return 1;
    }

    /**
     * This is the activity function that fails the first try and is then revived.
     */
    @FunctionName("RequestSecondaryApproval")
    public int requestSecondaryApproval(
            @DurableActivityTrigger(name = "name") int number,
            final ExecutionContext context) throws InterruptedException {
        return number / approvalFlag++;
    }

    /**
     * This HTTP-triggered function rewinds the orchestration using instanceId.
     */
    @FunctionName("RewindInstance")
    public String rewindInstance(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String instanceId = request.getQueryParameters().getOrDefault("instanceId", "");
        String reason = "Orchestrator failed and needs to be revived.";

        DurableTaskClient client = durableContext.getClient();
        client.rewindInstance(instanceId, reason);
        return "Failed orchestration instance is scheduled for rewind.";
    }

    /**
     * This HTTP-triggered function resets the approvalFlag variable for testing purposes.
     */
    @FunctionName("ResetApproval")
    public static HttpResponseMessage resetApproval(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("ResetApproval function invoked.");
        approvalFlag = 0;
        return request.createResponseBuilder(HttpStatus.OK).body(approvalFlag).build();
    }
}
