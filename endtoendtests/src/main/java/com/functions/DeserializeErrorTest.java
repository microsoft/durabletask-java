package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.util.Optional;

public class DeserializeErrorTest {
  @FunctionName("DeserializeErrorHttp")
  public HttpResponseMessage deserializeErrorHttp(
      @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
      HttpRequestMessage<Optional<String>> request,
      @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
      final ExecutionContext context) {
    context.getLogger().info("Java HTTP trigger processed a request.");

    DurableTaskClient client = durableContext.getClient();
    String instanceId = client.scheduleNewOrchestrationInstance("DeserializeErrorOrchestrator");
    context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
    return durableContext.createCheckStatusResponse(request, instanceId);
  }

  @FunctionName("DeserializeErrorOrchestrator")
  public String deserializeErrorOrchestrator(
      @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
    // cause deserialize error
    Person result = ctx.callActivity("Capitalize", "Austin", Person.class).await();
    return result.getName();
  }

  @FunctionName("SubCompletedErrorHttp")
  public HttpResponseMessage subCompletedErrorHttp(
      @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
      @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
      final ExecutionContext context) {
    context.getLogger().info("Java HTTP trigger processed a request.");

    DurableTaskClient client = durableContext.getClient();
    String instanceId = client.scheduleNewOrchestrationInstance("CompletedErrorOrchestrator");
    context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
    return durableContext.createCheckStatusResponse(request, instanceId);
  }

  @FunctionName("CompletedErrorOrchestrator")
  public String completedErrorOrchestrator(
      @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
    // cause deserialize issue
    Person result = ctx.callSubOrchestrator("CompletedErrorSubOrchestrator", "Austin", Person.class).await();
    return result.getName();
  }

  @FunctionName("CompletedErrorSubOrchestrator")
  public String completedErrorSubOrchestrator(
      @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
    return "test";
  }

  @FunctionName("ExternalEventHttp")
  public HttpResponseMessage externalEventHttp(
      @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
      @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
      final ExecutionContext context) {
    context.getLogger().info("Java HTTP trigger processed a request.");

    DurableTaskClient client = durableContext.getClient();
    String instanceId = client.scheduleNewOrchestrationInstance("ExternalEventActivity");
    context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
    return durableContext.createCheckStatusResponse(request, instanceId);
  }

  @FunctionName("ExternalEventActivity")
  public void externalEventActivity(@DurableOrchestrationTrigger(name = "runtimeState") TaskOrchestrationContext ctx)
  {
    System.out.println("Waiting external event...");
    Task<String> event = ctx.waitForExternalEvent("event", String.class);
    Task<?> result = ctx.anyOf(event).await();
    Object input = result.await();
    System.out.println(input);
  }

  static class Person {
    String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }
}
